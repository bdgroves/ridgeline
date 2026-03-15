"""
ridgeline / src / ingest / fetch_gis.py

Pulls real GIS layers from Maricopa, Phoenix, and Pima county ArcGIS REST APIs.
No auth required — all public endpoints.

Verified layer registry (March 2026):
  Maricopa County — gis.maricopa.gov/arcgis/rest/services/PNR/ParkAndTrail/MapServer
    0  Parks polygons
    2  Trailheads (points)
    7  Trails by name (lines)

  City of Phoenix — maps.phoenix.gov/pub/rest/services/Public/ParksOpenData/MapServer
    10  Park Boundaries (polygons)
    11  Walk Phoenix Trails (lines)

  Pima County — gisdata.pima.gov/arcgis1/rest/services/GISOpenData/Community2/MapServer
    6   Trailheads and Trail Access Points (points)
    7   Trails (lines)

  Pima County — gisdata.pima.gov/arcgis1/rest/services/GISOpenData/Environmental/MapServer
    3   Protected Lands of Pima County (polygons)

Run:
    pixi run gis
    python src/ingest/fetch_gis.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import geopandas as gpd
import httpx
import pandas as pd
from rich.console import Console
from rich.table import Table

ROOT    = Path(__file__).resolve().parents[2]
EXT_DIR = ROOT / "data" / "external"
EXT_DIR.mkdir(parents=True, exist_ok=True)

console = Console()

# ── Verified layer registry ────────────────────────────────────────────────
LAYERS = [
    # (label, base_url, layer_id, output_file, county)
    (
        "Maricopa Parks",
        "https://gis.maricopa.gov/arcgis/rest/services/PNR/ParkAndTrail/MapServer",
        0, "maricopa_parks.geojson", "Maricopa",
    ),
    (
        "Maricopa Trailheads",
        "https://gis.maricopa.gov/arcgis/rest/services/PNR/ParkAndTrail/MapServer",
        2, "maricopa_trailheads.geojson", "Maricopa",
    ),
    (
        "Maricopa Trails",
        "https://gis.maricopa.gov/arcgis/rest/services/PNR/ParkAndTrail/MapServer",
        7, "maricopa_trails.geojson", "Maricopa",
    ),
    (
        "Phoenix Park Boundaries",
        "https://maps.phoenix.gov/pub/rest/services/Public/ParksOpenData/MapServer",
        10, "phoenix_parks.geojson", "Maricopa",
    ),
    (
        "Phoenix Walk Trails",
        "https://maps.phoenix.gov/pub/rest/services/Public/ParksOpenData/MapServer",
        11, "phoenix_trails.geojson", "Maricopa",
    ),
    (
        "Pima Trailheads",
        "https://gisdata.pima.gov/arcgis1/rest/services/GISOpenData/Community2/MapServer",
        6, "pima_trailheads.geojson", "Pima",
    ),
    (
        "Pima Trails",
        "https://gisdata.pima.gov/arcgis1/rest/services/GISOpenData/Community2/MapServer",
        7, "pima_trails.geojson", "Pima",
    ),
    (
        "Pima Protected Lands",
        "https://gisdata.pima.gov/arcgis1/rest/services/GISOpenData/Environmental/MapServer",
        3, "pima_protected_lands.geojson", "Pima",
    ),
]

# WUI study area bbox (WGS84)
AZ_WUI_BBOX = "-113.0,31.5,-110.0,34.0"


def build_query_url(base_url: str, layer_id: int) -> str:
    return (
        f"{base_url}/{layer_id}/query"
        f"?where=1%3D1"
        f"&outFields=*"
        f"&geometry={AZ_WUI_BBOX}"
        f"&geometryType=esriGeometryEnvelope"
        f"&inSR=4326"
        f"&spatialRel=esriSpatialRelIntersects"
        f"&resultRecordCount=2000"
        f"&f=geojson"
    )


def fetch_layer(
    label: str, base_url: str, layer_id: int,
    outfile: str, county: str, retries: int = 3,
) -> gpd.GeoDataFrame | None:
    url  = build_query_url(base_url, layer_id)
    dest = EXT_DIR / outfile

    for attempt in range(retries):
        try:
            r = httpx.get(url, timeout=45, follow_redirects=True)
            r.raise_for_status()
            data = r.json()
            features = data.get("features", [])

            if not features:
                console.print(f"  [yellow]⚠[/yellow]  {label}: 0 features returned")
                return None

            gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
            gdf["county"]      = county
            gdf["layer_label"] = label
            dest.write_text(json.dumps(data, indent=2))
            console.print(f"  [green]✓[/green] {label}: [cyan]{len(gdf):,}[/cyan] features → {outfile}")
            return gdf

        except httpx.HTTPStatusError as e:
            console.print(f"  [yellow]⚠[/yellow]  {label}: HTTP {e.response.status_code} (attempt {attempt+1})")
        except Exception as exc:
            console.print(f"  [yellow]⚠[/yellow]  {label}: {exc} (attempt {attempt+1})")

        if attempt < retries - 1:
            time.sleep(2 ** attempt)

    return None


def merge_trailheads(results: dict[str, gpd.GeoDataFrame | None]) -> gpd.GeoDataFrame | None:
    """Combine all trailhead point layers into one unified GeoDataFrame."""
    frames = []

    # Maricopa trailheads
    gdf = results.get("maricopa_trailheads")
    if gdf is not None:
        name_col = next((c for c in ["TrailheadName","Name","NAME","TRAIL_NAME"]
                         if c in gdf.columns), None)
        gdf = gdf.copy()
        gdf["name"] = gdf[name_col] if name_col else "Maricopa Trailhead"
        gdf["source_layer"] = "Maricopa PNR"
        frames.append(gdf[["name","county","geometry","source_layer"]])

    # Pima trailheads
    gdf = results.get("pima_trailheads")
    if gdf is not None:
        name_col = next((c for c in ["NAME","Name","TRAIL_NAME","TrailName","TRAILHEAD_NAME"]
                         if c in gdf.columns), None)
        gdf = gdf.copy()
        gdf["name"] = gdf[name_col] if name_col else "Pima Trailhead"
        gdf["source_layer"] = "Pima Community2"
        frames.append(gdf[["name","county","geometry","source_layer"]])

    # Phoenix parks centroids as proxy trailheads
    gdf = results.get("phoenix_parks")
    if gdf is not None:
        name_col = next((c for c in ["NAME","Name","PROPERTY_NAME","PARK_NAME"]
                         if c in gdf.columns), None)
        gdf = gdf.copy()
        gdf["geometry"]     = gdf["geometry"].centroid
        gdf["name"]         = gdf[name_col] if name_col else "Phoenix Park"
        gdf["source_layer"] = "Phoenix Parks"
        frames.append(gdf[["name","county","geometry","source_layer"]])

    if not frames:
        console.print("  [yellow]⚠[/yellow]  No trailhead layers available to combine")
        return None

    combined = gpd.GeoDataFrame(
        pd.concat(frames, ignore_index=True), crs="EPSG:4326"
    )
    combined["lon"] = combined.geometry.x
    combined["lat"] = combined.geometry.y
    combined = combined[
        combined["lat"].between(31.0, 37.0) &
        combined["lon"].between(-115.0, -109.0)
    ].copy()

    out = EXT_DIR / "trailheads_combined.geojson"
    combined.to_file(out, driver="GeoJSON")
    console.print(f"\n  [green]✓[/green] Combined trailheads: [cyan]{len(combined):,}[/cyan] → trailheads_combined.geojson")
    return combined


def save_summary(results: dict[str, gpd.GeoDataFrame | None]) -> None:
    rows = [
        {"layer": k, "features": len(v) if v is not None else 0,
         "status": "ok" if v is not None else "failed"}
        for k, v in results.items()
    ]
    df = pd.DataFrame(rows)
    df.to_csv(EXT_DIR / "gis_fetch_summary.csv", index=False)

    t = Table(title="GIS Layers", show_header=True, header_style="bold")
    t.add_column("Layer",    style="cyan")
    t.add_column("Features", justify="right")
    t.add_column("Status")
    for _, r in df.iterrows():
        st = "[green]✓[/green]" if r["status"] == "ok" else "[red]✗[/red]"
        t.add_row(r["layer"], str(r["features"]), st)
    console.print(t)


def main() -> None:
    console.rule("[bold]RIDGELINE — GIS Data Fetch[/bold]")
    console.print("  Maricopa County · City of Phoenix · Pima County — ArcGIS REST\n")

    results: dict[str, gpd.GeoDataFrame | None] = {}
    for label, base_url, layer_id, outfile, county in LAYERS:
        key = outfile.replace(".geojson", "")
        results[key] = fetch_layer(label, base_url, layer_id, outfile, county)
        time.sleep(0.4)

    trailheads = merge_trailheads(results)
    if trailheads is not None:
        results["trailheads_combined"] = trailheads

    save_summary(results)
    console.rule("[green]GIS fetch done[/green]")


if __name__ == "__main__":
    main()
