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

import json
import time
from pathlib import Path

import httpx
import pandas as pd
from rich.console import Console

from addresses import direction_conflict, normalize, preserve_site, street_words, type_conflict
from rich.progress import (
    BarColumn, MofNCompleteColumn, Progress,
    SpinnerColumn, TextColumn, TimeElapsedColumn,
)

ROOT     = Path(__file__).resolve().parents[2]
PROC_DIR = ROOT / "data" / "processed"
EXT_DIR  = ROOT / "data" / "external"

# Committed, so CI and local runs only query addresses they haven't seen.
CACHE_PATH  = EXT_DIR / "geocode_cache.csv"
REPORT_PATH = EXT_DIR / "geocode_report.json"
CACHE_COLS  = ["address", "query", "precision", "latitude", "longitude",
               "score", "match_addr", "addr_type", "method", "version"]

# Bump when matching logic changes; cached misses from older versions are
# re-queried, cached hits are kept.
GEOCODER_VERSION = 4

# Scores 70-80 on these match types are usually the right street with an
# out-of-range house number (hundred-block midpoints often don't exist).
# Accepted only when the matched address contains the street we asked for.
NEAR_MISS_MIN   = 70
NEAR_MISS_TYPES = {"PointAddress", "StreetAddress", "StreetAddressExt",
                   "StreetName", "StreetInt"}

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

# Preserve fallbacks live in addresses.py (see the note there on street names).


def geocode_address(address: str, client: httpx.Client, city: str = "Phoenix") -> dict:
    """
    Geocode one Phoenix Fire address. Returns a cache row.

    Order matters: the Maricopa geocoder is tried first on the normalized
    query (hundred block -> midpoint, "A/B" -> "A & B"). Only if that fails
    does a preserve name fall back to that preserve's representative point,
    and only for patterns that mean the preserve rather than a street named
    after it. The old code ran the fallback first on bare keywords, which
    pinned calls on Camelback Rd and McDowell Rd to the preserves.
    """
    row = {"address": address, "query": None, "precision": None,
           "latitude": None, "longitude": None, "score": None,
           "match_addr": None, "addr_type": None, "method": "failed",
           "version": GEOCODER_VERSION}
    if not isinstance(address, str) or not address.strip():
        return row

    query, precision = normalize(address)
    row.update(query=query, precision=precision)

    try:
        params = {
            "SingleLine": f"{query}, {city}, AZ",
            "outFields":  "Score,Match_addr,Addr_type",
            "maxLocations": 1,
            "outSR": "4326",   # return decimal degrees, not Web Mercator
            "f": "json",
        }
        r = client.get(GEOCODE_URL, params=params, timeout=10)
        r.raise_for_status()
        candidates = r.json().get("candidates", [])
        if candidates:
            cand  = candidates[0]
            score = cand.get("score", 0)
            attrs = cand.get("attributes", {})
            match = str(attrs.get("Match_addr") or cand.get("address") or "")
            atype = attrs.get("Addr_type")
            row.update(score=score, match_addr=match, addr_type=atype)
            loc = cand["location"]
            if direction_conflict(query, match):
                pass                      # right street, wrong side of town
            elif score >= 80:
                row.update(latitude=loc["y"], longitude=loc["x"], method="geocoder")
                return row
            same_street = all(ws & set(match.upper().replace(",", " ").split())
                              for ws in street_words(query))
            if (score >= NEAR_MISS_MIN and atype in NEAR_MISS_TYPES and same_street
                    and not direction_conflict(query, match) and not type_conflict(query, match)):
                row.update(latitude=loc["y"], longitude=loc["x"], method="geocoder_street")
                return row
    except Exception:
        errored = True
    else:
        errored = False

    site = preserve_site(address)
    if site:
        name, (lat, lon) = site
        row.update(latitude=lat, longitude=lon, precision="preserve_centroid",
                   method=f"preserve:{name}")
    if errored:
        row["method"] = "error"      # not trusted from cache; retried next run
    return row


def load_cache() -> dict[str, dict]:
    if not CACHE_PATH.exists():
        return {}
    c = pd.read_csv(CACHE_PATH, dtype={"address": str})
    if "version" not in c.columns:
        c["version"] = 1
    stale_miss = c["method"].isin(["failed", "error"]) & (c["version"].fillna(1) < GEOCODER_VERSION)
    # Hits made before the direction check existed are re-validated.
    bad_dir = c["method"].isin(["geocoder", "geocoder_street"]) & c.apply(
        lambda r: direction_conflict(str(r.get("query") or ""), str(r.get("match_addr") or "")), axis=1)
    # Near misses made before the street-type check are re-validated too.
    bad_type = (c["method"] == "geocoder_street") & c.apply(
        lambda r: type_conflict(str(r.get("query") or ""), str(r.get("match_addr") or "")), axis=1)
    c = c[(c["method"] != "error") & ~stale_miss & ~bad_dir & ~bad_type]
    return {r["address"]: r for r in c.to_dict(orient="records")}


def save_cache(cache: dict[str, dict]) -> None:
    rows = sorted(cache.values(), key=lambda r: str(r["address"]))
    tmp = CACHE_PATH.with_suffix(".tmp")
    pd.DataFrame(rows, columns=CACHE_COLS).to_csv(tmp, index=False)
    tmp.replace(CACHE_PATH)


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


def write_report(df: pd.DataFrame, to_geocode: pd.DataFrame) -> None:
    """Committed summary so match rates can be checked without the raw data."""
    def method_group(m):
        m = str(m)
        return "preserve_centroid" if m.startswith("preserve:") else m
    tg = to_geocode.assign(method=to_geocode["geo_method"].map(method_group),
                           nature=to_geocode["incident_type"].str.lower())
    mr_all = df[df["incident_type"].str.lower() == "mountain rescue"]
    mr = tg[tg["nature"] == "mountain rescue"]
    report = {
        "incidents_in_clean_set": int(len(df)),
        "priority_subset": int(len(tg)),
        "located": int(tg["latitude"].notna().sum()),
        "by_method": {k: int(v) for k, v in tg["method"].value_counts().items()},
        "by_precision": {str(k): int(v) for k, v in tg["geo_precision"].value_counts(dropna=False).items()},
        "mountain_rescue": {
            "rows": int(len(mr_all)),
            "unique_incident_ids": int(mr_all["incident_id"].nunique()),
            "located": int(mr["latitude"].notna().sum()),
            "by_method": {k: int(v) for k, v in mr["method"].value_counts().items()},
            "top_unlocated_addresses": [
                [a, int(n)] for a, n in
                mr[mr["latitude"].isna()]["location_name"].value_counts().head(25).items()
            ],
        },
        "by_nature_located_pct": {
            k: round(float(g["latitude"].notna().mean()) * 100, 1)
            for k, g in tg.groupby("nature") if len(g) >= 20
        },
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2))
    console.print(f"  [green]✓[/green] Report → {REPORT_PATH.name}")


def main() -> None:
    console.rule("[bold]RIDGELINE — Geocoding SAR Incidents (PHX + LA)[/bold]")

    # ── Phoenix ────────────────────────────────────────────────────────────
    parquet = PROC_DIR / "phoenix_fire_sar_clean.parquet"
    if not parquet.exists():
        console.print("[yellow]No Phoenix data — run `pixi run phoenix` first.[/yellow]")

    df = pd.read_parquet(parquet)
    console.print(f"  Loaded: [cyan]{len(df):,}[/cyan] incidents")

    # Filter to priority nature codes + preserve addresses
    nature_mask = df["incident_type"].str.lower().isin(PRIORITY_NATURE_CODES)
    preserve_mask = df["location_name"].map(lambda a: preserve_site(a) is not None)
    to_geocode = df[nature_mask | preserve_mask].copy()
    console.print(f"  Priority subset for geocoding: [cyan]{len(to_geocode):,}[/cyan]")

    unique_addresses = to_geocode["location_name"].dropna().unique()
    cache = load_cache()
    todo = [a for a in unique_addresses if a not in cache]
    console.print(f"  Unique addresses: [cyan]{len(unique_addresses):,}[/cyan] "
                  f"· cached [cyan]{len(unique_addresses) - len(todo):,}[/cyan] "
                  f"· to query [cyan]{len(todo):,}[/cyan]\n")

    with httpx.Client() as client:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as prog:
            task = prog.add_task("Geocoding…", total=len(todo))
            for i, addr in enumerate(todo, 1):
                cache[addr] = geocode_address(addr, client)
                time.sleep(0.05)          # be polite to the county server
                if i % 500 == 0:
                    save_cache(cache)     # don't lose work if the run dies
                prog.advance(task)
    save_cache(cache)

    looked = {a: cache[a] for a in unique_addresses if a in cache}
    to_geocode["latitude"]  = to_geocode["location_name"].map(lambda a: looked.get(a, {}).get("latitude"))
    to_geocode["longitude"] = to_geocode["location_name"].map(lambda a: looked.get(a, {}).get("longitude"))
    to_geocode["geo_precision"] = to_geocode["location_name"].map(lambda a: looked.get(a, {}).get("precision"))
    to_geocode["geo_method"]    = to_geocode["location_name"].map(lambda a: looked.get(a, {}).get("method"))

    geocoded = to_geocode[to_geocode["latitude"].notna()].copy()
    console.print(f"  Incidents with coordinates: [cyan]{len(geocoded):,}[/cyan] "
                  f"of {len(to_geocode):,}")

    out = PROC_DIR / "phoenix_fire_sar_geocoded.parquet"
    geocoded.to_parquet(out, index=False)
    console.print(f"\n  [green]✓[/green] Saved → {out.name}")

    write_report(df, to_geocode)

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
