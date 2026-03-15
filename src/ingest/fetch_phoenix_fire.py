"""
ridgeline / src / ingest / fetch_phoenix_fire.py

Downloads Phoenix Fire Department Calls for Service CSVs (public, free,
no auth) and filters for mountain rescue / SAR-relevant incidents.

Source: phoenixopendata.com — Creative Commons Attribution license
Years available: 2014–2025, updated monthly.

Nature codes we filter for (case-insensitive substring match on NATURE_TEXT):
    mountain, rescue, hiker, trail, heat, stranded, technical rescue,
    swift water, flood, mtn, backcountry, wilderness

Output → data/raw/phoenix_fire_sar_raw.csv
         data/processed/phoenix_fire_sar_clean.parquet

Run:
    pixi run phoenix
    python src/ingest/fetch_phoenix_fire.py
"""

from __future__ import annotations

import io
from pathlib import Path

import httpx
import pandas as pd
from rich.console import Console
from rich.table import Table

ROOT     = Path(__file__).resolve().parents[2]
RAW_DIR  = ROOT / "data" / "raw"
PROC_DIR = ROOT / "data" / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

console = Console()

# ── Direct CSV download URLs 2019–2024 ─────────────────────────────────────
# All Creative Commons Attribution licensed
YEAR_URLS = {
    2019: "https://www.phoenixopendata.com/dataset/caf49f72-f22f-4ad9-9405-2a3db9619423/resource/45b13b01-d1c5-4159-b313-8d409dd431cb/download/calls-for-service-fire_calls-for-service-2019_calls-for-service.csv",
    2020: "https://www.phoenixopendata.com/dataset/caf49f72-f22f-4ad9-9405-2a3db9619423/resource/d0164e0f-8af4-4bbe-99f6-f952717aaf36/download/calls-for-service-fire_calls-for-service-2020_cfs2020.csv",
    2021: "https://www.phoenixopendata.com/dataset/caf49f72-f22f-4ad9-9405-2a3db9619423/resource/6b57764d-bb95-4c1e-85b3-77e3d0f14841/download/calls-for-service-fire_calls-for-service-2021_calls-for-service.csv",
    2022: "https://www.phoenixopendata.com/dataset/caf49f72-f22f-4ad9-9405-2a3db9619423/resource/f32a4ba0-0c18-45eb-b0c7-4d46170fbcb9/download/calls-for-service-fire_calls-for-service-2022_cfs2022.csv",
    2023: "https://www.phoenixopendata.com/dataset/caf49f72-f22f-4ad9-9405-2a3db9619423/resource/e832854c-6537-4223-ba26-674a7b799f49/download/calls-for-service-fire_calls-for-service-2023_calls-for-service.csv",
    2024: "https://www.phoenixopendata.com/dataset/caf49f72-f22f-4ad9-9405-2a3db9619423/resource/2169fba5-a64a-42da-893d-931b97ea10ef/download/calls-for-service-fire_calls-for-service-2024_calls_for_service.csv",
    2025: "https://www.phoenixopendata.com/dataset/caf49f72-f22f-4ad9-9405-2a3db9619423/resource/3f3bb1b6-dfe3-4b69-9a5d-cedef4264087/download/calls-for-service-fire_calls-for-service-2025_calls_for_service.csv",
}

# ── SAR filter terms ────────────────────────────────────────────────────────
# Match against NATURE_TEXT (lower) or NATURE_CODE (lower)
SAR_TERMS = [
    "mountain", "mtn", "rescue", "hiker", "hiking",
    "trail", "technical rescue", "swift water", "swiftwater",
    "flood", "stranded", "backcountry", "wilderness",
    "heat exhaustion", "heat stroke", "heat emergency",
    "lost person", "search", "overdue",
]

# Address keywords that suggest WUI / mountain locations
WUI_ADDRESS_TERMS = [
    "camelback", "piestewa", "south mountain", "mcdowell",
    "phoenix mountain", "echo canyon", "cholla", "holbert",
    "shaw butte", "north mountain", "dreamy draw",
    "white tank", "estrella", "usery",
]


def fetch_year(year: int, url: str) -> pd.DataFrame | None:
    """Download one year CSV and return as DataFrame."""
    console.print(f"  [cyan]→[/cyan] {year}: downloading …", end="")
    try:
        r = httpx.get(url, timeout=60, follow_redirects=True)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        console.print(f" [green]{len(df):,} rows[/green]")
        return df
    except Exception as exc:
        console.print(f" [red]failed: {exc}[/red]")
        return None


def filter_sar(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Filter full calls-for-service to SAR-relevant incidents using:
    1. NATURE_TEXT / NATURE_CODE keyword match
    2. INCIDENT_ADDRESS WUI location keyword match
    """
    df = df.copy()
    df.columns = [c.strip().upper() for c in df.columns]

    # Normalise text columns
    nature_text = df.get("NATURE_TEXT", pd.Series(dtype=str)).fillna("").str.lower()
    nature_code = df.get("NATURE_CODE", pd.Series(dtype=str)).fillna("").str.lower()
    address     = df.get("INCIDENT_ADDRESS", pd.Series(dtype=str)).fillna("").str.lower()

    # Match SAR terms in nature text or code
    sar_mask = pd.Series(False, index=df.index)
    for term in SAR_TERMS:
        sar_mask |= nature_text.str.contains(term, na=False)
        sar_mask |= nature_code.str.contains(term, na=False)

    # Match WUI address terms
    wui_mask = pd.Series(False, index=df.index)
    for term in WUI_ADDRESS_TERMS:
        wui_mask |= address.str.contains(term, na=False)

    combined = sar_mask | wui_mask
    filtered = df[combined].copy()
    filtered["source_year"]   = year
    filtered["filter_method"] = "nature_text"
    filtered.loc[wui_mask & ~sar_mask, "filter_method"] = "wui_address"
    filtered.loc[wui_mask & sar_mask,  "filter_method"] = "both"

    return filtered


def standardise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map Phoenix Fire schema to RIDGELINE standard schema.
    Adds lat/lon placeholder (Phoenix fire data has address, not coords).
    """
    df = df.copy()

    # Parse datetime
    reported_col = next((c for c in df.columns if "REPORTED" in c), None)
    closed_col   = next((c for c in df.columns if "CLOSED" in c), None)

    if reported_col:
        df["datetime"] = pd.to_datetime(df[reported_col], errors="coerce", format="mixed")
        df["date"]     = df["datetime"].dt.date.astype(str)
        df["year"]     = df["datetime"].dt.year
        df["month"]    = df["datetime"].dt.month
        df["hour"]     = df["datetime"].dt.hour
        df["day_of_week_num"] = df["datetime"].dt.dayofweek
        df["is_weekend"]      = df["day_of_week_num"] >= 5

        # Duration
        if closed_col:
            df["closed_dt"]      = pd.to_datetime(df[closed_col], errors="coerce", format="mixed")
            df["duration_hours"] = ((df["closed_dt"] - df["datetime"])
                                    .dt.total_seconds() / 3600).round(2)
            df["duration_hours"] = df["duration_hours"].clip(0, 48)

    # Time of day bucket
    df["time_of_day_bucket"] = df["hour"].map(
        lambda h: ("dawn"  if  5 <= h <  9 else
                   "day"   if  9 <= h < 17 else
                   "dusk"  if 17 <= h < 21 else
                   "night") if pd.notna(h) else None
    )

    # Standard columns
    df["incident_id"]    = df.get("INCIDENT", pd.Series(dtype=str)).astype(str)
    df["location_name"]  = df.get("INCIDENT_ADDRESS", pd.Series(dtype=str))
    df["incident_type"]  = df.get("NATURE_TEXT", pd.Series(dtype=str))
    df["nature_code"]    = df.get("NATURE_CODE", pd.Series(dtype=str))
    df["county"]         = "Maricopa"
    df["state"]          = "AZ"
    df["data_source"]    = "phoenix_fire_opendata"

    # Infer behavioral cluster from nature text
    def _cluster(row):
        t = str(row.get("incident_type","")).lower()
        a = str(row.get("location_name","")).lower()
        if any(x in t for x in ["heat exhaustion","heat stroke","heat"]):
            return "recreational_underequipped"
        if any(x in t for x in ["swift water","flood","stranded"]):
            return "flash_flood_stranded"
        if any(x in t for x in ["lost","search","overdue"]):
            return "casual_proximity"
        if any(x in a for x in ["camelback","piestewa","south mountain","echo canyon","cholla"]):
            return "recreational_underequipped"
        if any(x in a for x in ["mcdowell","white tank","estrella","usery"]):
            return "recreational_underequipped"
        return "recreational_underequipped"  # default for Phoenix mountain rescues

    df["behavioral_cluster"] = df.apply(_cluster, axis=1)

    # Lat/lon — Phoenix fire data is address only, no coords
    # Will be geocoded in a future step; set to None for now
    df["latitude"]  = None
    df["longitude"] = None

    return df[[
        "incident_id","datetime","date","year","month","hour",
        "day_of_week_num","is_weekend","time_of_day_bucket",
        "location_name","county","state","latitude","longitude",
        "incident_type","nature_code","duration_hours",
        "behavioral_cluster","filter_method","data_source","source_year",
    ]]


def main() -> None:
    console.rule("[bold]RIDGELINE — Phoenix Fire Real Data[/bold]")
    console.print("  Source: phoenixopendata.com · CC Attribution\n")

    all_frames = []
    year_counts = {}

    for year, url in sorted(YEAR_URLS.items()):
        raw = fetch_year(year, url)
        if raw is None:
            year_counts[year] = 0
            continue

        filtered = filter_sar(raw, year)
        year_counts[year] = len(filtered)

        if not filtered.empty:
            all_frames.append(filtered)
            console.print(
                f"    [green]✓[/green] {year}: "
                f"[cyan]{len(filtered):,}[/cyan] SAR-relevant incidents "
                f"({len(filtered)/len(raw)*100:.2f}% of {len(raw):,} total)"
            )

    if not all_frames:
        console.print("[red]No data fetched — check network and URLs[/red]")
        return

    combined = pd.concat(all_frames, ignore_index=True)

    # Save raw filtered
    raw_out = RAW_DIR / "phoenix_fire_sar_raw.csv"
    combined.to_csv(raw_out, index=False)
    console.print(f"\n  [green]✓[/green] Raw → {raw_out.name} ({raw_out.stat().st_size:,} bytes)")

    # Standardise and save parquet
    clean = standardise(combined)
    pq_out = PROC_DIR / "phoenix_fire_sar_clean.parquet"
    clean.to_parquet(pq_out, index=False)
    console.print(f"  [green]✓[/green] Clean parquet → {pq_out.name}")

    # ── Summary ────────────────────────────────────────────────────────────
    t = Table(title="Phoenix Fire SAR Incidents — Real Data", header_style="bold")
    t.add_column("Year",     style="cyan")
    t.add_column("Incidents", justify="right")
    for yr, cnt in sorted(year_counts.items()):
        t.add_row(str(yr), f"[green]{cnt:,}[/green]" if cnt else "[red]0[/red]")
    t.add_row("TOTAL", f"[bold]{len(combined):,}[/bold]")
    console.print(t)

    # Top nature codes
    top = (combined["NATURE_TEXT"].str.lower()
                                  .value_counts()
                                  .head(15)
                                  .reset_index())
    top.columns = ["nature_text", "count"]
    console.print("\n[bold]Top SAR nature codes in real data:[/bold]")
    for _, r in top.iterrows():
        console.print(f"  {r['nature_text']:<40} {r['count']:>5}")

    console.rule("[green]Phoenix Fire data pull done[/green]")


if __name__ == "__main__":
    main()
