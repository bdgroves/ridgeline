"""
ridgeline / src / ingest / geocode_phoenix.py

Geocodes Phoenix Fire incident addresses using the Maricopa County
geocoder REST API (free, no auth required).

Input:  data/processed/phoenix_fire_sar_clean.parquet
Output: data/processed/phoenix_fire_sar_geocoded.parquet

Only geocodes mountain rescue + water rescue + key SAR nature codes
to keep the API calls focused and fast.

Run:
    pixi run geocode
    python src/ingest/geocode_phoenix.py
"""

from __future__ import annotations

import time
from pathlib import Path

import httpx
import pandas as pd
from rich.console import Console
from rich.progress import (
    BarColumn, MofNCompleteColumn, Progress,
    SpinnerColumn, TextColumn, TimeElapsedColumn,
)

ROOT     = Path(__file__).resolve().parents[2]
PROC_DIR = ROOT / "data" / "processed"

console = Console()

# Maricopa County composite geocoder — free, no key
GEOCODE_URL = "https://gis.maricopa.gov/arcgis/rest/services/Geocode/MaricopaCountyGeocodeService/GeocodeServer/findAddressCandidates"

# Focus geocoding on the most SAR-relevant nature codes
PRIORITY_NATURE_CODES = {
    "mountain rescue", "water rescue", "swift water rescue",
    "technical rescue", "search", "lost person",
    "heat exhaustion", "heat stroke", "heat emergency",
    "check flooding condition",
}

# Known mountain/preserve address fragments → rough centroid fallback
# Used when geocoder returns no result
PRESERVE_FALLBACKS = {
    "camelback":      (33.5194, -111.9749),
    "piestewa":       (33.5307, -112.0197),
    "south mountain": (33.3476, -112.0540),
    "mcdowell":       (33.6918, -111.7951),
    "echo canyon":    (33.5194, -111.9749),
    "cholla":         (33.5244, -111.9603),
    "north mountain": (33.5710, -112.0580),
    "shaw butte":     (33.5791, -112.1020),
    "white tank":     (33.5971, -112.5476),
    "estrella":       (33.4317, -112.4076),
    "usery":          (33.4754, -111.6218),
    "dreamy draw":    (33.5460, -112.0260),
}


def geocode_address(address: str, client: httpx.Client) -> tuple[float, float] | None:
    """
    Geocode one address string. Returns (lat, lon) or None.
    Uses Maricopa County geocoder first, then preserve fallback.
    """
    if not isinstance(address, str) or not address.strip():
        return None

    # Check preserve fallback first — fast path for known mountain addresses
    addr_lower = address.lower()
    for keyword, coords in PRESERVE_FALLBACKS.items():
        if keyword in addr_lower:
            return coords

    # Try Maricopa County geocoder
    try:
        params = {
            "SingleLine": address + ", Phoenix, AZ",
            "outFields":  "Score",
            "maxLocations": 1,
            "outSR": "4326",   # return decimal degrees, not Web Mercator
            "f": "json",
        }
        r = client.get(GEOCODE_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        candidates = data.get("candidates", [])
        if candidates and candidates[0].get("score", 0) >= 80:
            loc = candidates[0]["location"]
            return (loc["y"], loc["x"])   # lat, lon
    except Exception:
        pass

    return None


LA_PRESERVE_FALLBACKS = {
    "runyon":          (34.1089, -118.3617),
    "griffith":        (34.1184, -118.3004),
    "topanga":         (34.0868, -118.5983),
    "malibu":          (34.0259, -118.7798),
    "fryman":          (34.1247, -118.3956),
    "temescal":        (34.0468, -118.5280),
    "mulholland":      (34.1139, -118.4068),
    "will rogers":     (34.0459, -118.5258),
    "eaton":           (34.1950, -118.0820),
    "altadena":        (34.1901, -118.1310),
    "angeles crest":   (34.2290, -118.1560),
    "palos verdes":    (33.7444, -118.3870),
    "la canada":       (34.1992, -118.1996),
    "canyon":          (34.0928, -118.3287),
}

LA_GEOCODE_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"


def geocode_la_address(address: str, client: httpx.Client) -> tuple[float, float] | None:
    """Geocode LA address using preserve fallbacks + Census geocoder."""
    if not isinstance(address, str) or not address.strip():
        return None
    addr_lower = address.lower()
    for keyword, coords in LA_PRESERVE_FALLBACKS.items():
        if keyword in addr_lower:
            return coords
    try:
        params = {
            "address":    address + ", Los Angeles, CA",
            "benchmark":  "Public_AR_Current",
            "format":     "json",
        }
        r = client.get(LA_GEOCODE_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        matches = data.get("result", {}).get("addressMatches", [])
        if matches:
            coords = matches[0]["coordinates"]
            return (coords["y"], coords["x"])
    except Exception:
        pass
    return None


def main() -> None:
    console.rule("[bold]RIDGELINE — Geocoding SAR Incidents (PHX + LA)[/bold]")

    # ── Phoenix ────────────────────────────────────────────────────────────
    parquet = PROC_DIR / "phoenix_fire_sar_clean.parquet"
    if not parquet.exists():
        console.print("[yellow]No Phoenix data — run `pixi run phoenix` first.[/yellow]")

    df = pd.read_parquet(parquet)
    console.print(f"  Loaded: [cyan]{len(df):,}[/cyan] incidents")

    # Filter to priority nature codes + WUI address matches
    nature_mask = df["incident_type"].str.lower().isin(PRIORITY_NATURE_CODES)
    preserve_mask = pd.Series(False, index=df.index)
    for kw in PRESERVE_FALLBACKS:
        preserve_mask |= df["location_name"].str.lower().str.contains(kw, na=False)

    to_geocode = df[nature_mask | preserve_mask].copy()
    console.print(f"  Priority subset for geocoding: [cyan]{len(to_geocode):,}[/cyan]")

    # Deduplicate addresses — many incidents at same location
    unique_addresses = to_geocode["location_name"].dropna().unique()
    console.print(f"  Unique addresses: [cyan]{len(unique_addresses):,}[/cyan]\n")

    # Geocode unique addresses
    addr_cache: dict[str, tuple[float, float] | None] = {}

    with httpx.Client() as client:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("Geocoding…", total=len(unique_addresses))

            for addr in unique_addresses:
                if addr not in addr_cache:
                    result = geocode_address(addr, client)
                    addr_cache[addr] = result
                    # Rate limit — be polite
                    time.sleep(0.05)
                prog.advance(task)

    # Map results back to full dataframe
    hits     = sum(1 for v in addr_cache.values() if v is not None)
    misses   = len(addr_cache) - hits
    console.print(f"\n  Geocoded: [green]{hits:,}[/green] ✓  |  Failed: [yellow]{misses:,}[/yellow]")

    to_geocode["latitude"]  = to_geocode["location_name"].map(
        lambda a: addr_cache.get(a, (None, None))[0] if addr_cache.get(a) else None
    )
    to_geocode["longitude"] = to_geocode["location_name"].map(
        lambda a: addr_cache.get(a, (None, None))[1] if addr_cache.get(a) else None
    )

    geocoded = to_geocode[to_geocode["latitude"].notna()].copy()
    console.print(f"  Incidents with coordinates: [cyan]{len(geocoded):,}[/cyan]")

    # Save
    out = PROC_DIR / "phoenix_fire_sar_geocoded.parquet"
    geocoded.to_parquet(out, index=False)
    console.print(f"\n  [green]✓[/green] Saved → {out.name}")

    # Quick breakdown
    if "incident_type" in geocoded.columns:
        top = (geocoded["incident_type"]
               .value_counts()
               .head(10))
        console.print("\n[bold]Geocoded incident types:[/bold]")
        for name, count in top.items():
            console.print(f"  {str(name):<45} {count:>5}")

    console.rule("[green]Geocoding done — Phoenix[/green]")


if __name__ == "__main__":
    main()
