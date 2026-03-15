"""
ridgeline / src / analysis / weather_pull.py

Pulls historical weather from Open-Meteo for each unique
incident date × location combination and joins to the cleaned dataset.

Uses the Open-Meteo historical API (free, no key required) — same
endpoint pattern as the weather-report-bot.

Adds columns to data/processed/sar_incidents_weather.parquet:
  temp_max_c, temp_max_f, precip_mm, wind_max_kmh,
  heat_index_f (approximate), is_extreme_heat (>= 110°F)

Run:
    pixi run weather
    python src/analysis/weather_pull.py
"""

from __future__ import annotations

import time
from pathlib import Path

import httpx
import pandas as pd
import numpy as np
from rich.console import Console
from rich.progress import (
    BarColumn, MofNCompleteColumn, Progress,
    SpinnerColumn, TextColumn, TimeElapsedColumn,
)

ROOT     = Path(__file__).resolve().parents[2]
PROC_DIR = ROOT / "data" / "processed"

console = Console()

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# Representative coordinates per location cluster
# (avoids hammering the API with per-record calls)
LOCATION_COORDS: dict[str, tuple[float, float]] = {
    "Maricopa": (33.4484, -112.0740),   # Phoenix centroid
    "Pima":     (32.2226, -110.9747),   # Tucson centroid
}


def c_to_f(c: float) -> float:
    return c * 9 / 5 + 32


def heat_index_f(t_f: float, rh: float) -> float:
    """Steadman heat index approximation (valid for T >= 80°F, RH >= 40%)."""
    if t_f < 80 or rh < 40:
        return t_f
    hi = (-42.379
          + 2.04901523  * t_f
          + 10.14333127 * rh
          - 0.22475541  * t_f * rh
          - 0.00683783  * t_f**2
          - 0.05481717  * rh**2
          + 0.00122874  * t_f**2 * rh
          + 0.00085282  * t_f * rh**2
          - 0.00000199  * t_f**2 * rh**2)
    return round(hi, 1)


def fetch_weather_for_county(
    county: str,
    dates: list[str],
    lat: float,
    lon: float,
    retries: int = 3,
) -> dict[str, dict]:
    """
    Fetch daily weather for a list of date strings (YYYY-MM-DD) at one location.
    Returns {date_str: {temp_max_c, precip_mm, wind_max_kmh, rh_mean}}.
    """
    if not dates:
        return {}

    start = min(dates)
    end   = max(dates)

    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start,
        "end_date":   end,
        "daily":      "temperature_2m_max,precipitation_sum,windspeed_10m_max,relativehumidity_2m_mean",
        "timezone":   "America/Phoenix",
        "temperature_unit": "celsius",
        "windspeed_unit":   "kmh",
        "precipitation_unit": "mm",
    }

    for attempt in range(retries):
        try:
            r = httpx.get(OPEN_METEO_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            daily = data.get("daily", {})
            result = {}
            for i, d in enumerate(daily.get("time", [])):
                result[d] = {
                    "temp_max_c":    daily["temperature_2m_max"][i],
                    "precip_mm":     daily["precipitation_sum"][i],
                    "wind_max_kmh":  daily["windspeed_10m_max"][i],
                    "rh_mean":       daily.get("relativehumidity_2m_mean", [None]*999)[i],
                }
            return result
        except Exception as exc:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                console.print(f"  [yellow]⚠[/yellow] Weather fetch failed for {county}: {exc}")
                return {}
    return {}


def main() -> None:
    console.rule("[bold]RIDGELINE — Weather Data Pull (Open-Meteo)[/bold]")

    parquet = PROC_DIR / "sar_incidents_clean.parquet"
    csv     = PROC_DIR / "sar_incidents_clean.csv"

    if parquet.exists():
        df = pd.read_parquet(parquet)
    elif csv.exists():
        df = pd.read_csv(csv, low_memory=False)
    else:
        console.print("[red]No processed data found. Run `pixi run pipeline` first.[/red]")
        return

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")
    df["county"]   = df["county"].str.strip().str.title()

    weather_rows: list[dict] = []

    with Progress(
        SpinnerColumn(), TextColumn("{task.description}"),
        BarColumn(), MofNCompleteColumn(), TimeElapsedColumn(),
        console=console,
    ) as prog:
        counties = [c for c in ["Maricopa", "Pima"]
                    if c in df["county"].unique()]
        task = prog.add_task("Fetching weather by county…", total=len(counties))

        for county in counties:
            lat, lon = LOCATION_COORDS.get(county, (33.45, -112.07))
            dates = sorted(df[df["county"] == county]["date_str"].unique().tolist())
            console.print(f"  {county}: {len(dates):,} unique dates")

            weather_data = fetch_weather_for_county(county, dates, lat, lon)

            for date_str, wx in weather_data.items():
                t_c  = wx.get("temp_max_c")
                t_f  = c_to_f(t_c) if t_c is not None else None
                rh   = wx.get("rh_mean") or 20.0  # AZ default low humidity
                hi_f = heat_index_f(t_f, rh) if t_f is not None else None

                weather_rows.append({
                    "county":           county,
                    "date_str":         date_str,
                    "temp_max_c":       round(t_c, 1) if t_c is not None else None,
                    "temp_max_f":       round(t_f, 1) if t_f is not None else None,
                    "precip_mm":        wx.get("precip_mm"),
                    "wind_max_kmh":     wx.get("wind_max_kmh"),
                    "rh_mean":          rh,
                    "heat_index_f":     hi_f,
                    "is_extreme_heat":  (t_f is not None and t_f >= 110),
                    "is_monsoon_precip": (wx.get("precip_mm") or 0) >= 12.7,
                })

            prog.advance(task)
            time.sleep(0.5)   # be polite to the API

    if not weather_rows:
        console.print("[yellow]No weather data fetched — API may be unreachable.[/yellow]")
        return

    wx_df = pd.DataFrame(weather_rows)

    # Join back to incidents
    df_merged = df.merge(
        wx_df.rename(columns={"date_str": "date_str"}),
        on=["county", "date_str"],
        how="left",
    )

    out_path = PROC_DIR / "sar_incidents_weather.parquet"
    df_merged.to_parquet(out_path, index=False)
    console.print(f"\n[green]✓[/green] Saved → {out_path.name}")
    console.print(f"  {wx_df['is_extreme_heat'].sum():,} extreme heat days in dataset")
    console.print(f"  {wx_df['is_monsoon_precip'].sum():,} monsoon-threshold precip days")
    console.rule("[green]Weather pull done[/green]")


if __name__ == "__main__":
    main()
