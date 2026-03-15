"""
ridgeline / src / viz / build_map.py

Builds an interactive folium map combining:
  - Real GIS layers (parks, preserves, trailheads) from Maricopa + Pima
  - Incident points from processed seed/real data
  - Cluster color coding
  - WUI edge zone overlay

Output → site/map.html  (included in GitHub Pages site)

Run:
    pixi run map
    python src/viz/build_map.py
"""

from __future__ import annotations

import json
from pathlib import Path

import folium
import geopandas as gpd
import pandas as pd
from folium.plugins import HeatMap
from rich.console import Console

ROOT     = Path(__file__).resolve().parents[2]
PROC_DIR = ROOT / "data" / "processed"
EXT_DIR  = ROOT / "data" / "external"
SITE_DIR = ROOT / "site"
SITE_DIR.mkdir(parents=True, exist_ok=True)

console = Console()

# ── Palette — matches sar_stats.py ────────────────────────────────────────
CLUSTER_COLORS = {
    "dog_walker":                   "#c8a96e",
    "casual_proximity":             "#4a8fa8",
    "party_spillover":              "#d94f1e",
    "youth_incident":               "#7a9e7e",
    "homeless_medical":             "#6b6a5e",
    "recreational_underequipped":   "#e8793a",
    "flash_flood_stranded":         "#5ba3c0",
    "unknown":                      "#3a4535",
}

CLUSTER_LABELS = {
    "dog_walker":                   "Dog Walker",
    "casual_proximity":             "Casual Proximity",
    "party_spillover":              "Party / Social Spillover",
    "youth_incident":               "Youth / Teen",
    "homeless_medical":             "Unhoused / Encampment",
    "recreational_underequipped":   "Recreational — Underequipped",
    "flash_flood_stranded":         "Flash Flood Stranded",
}


def load_incidents() -> pd.DataFrame:
    parquet = PROC_DIR / "sar_incidents_clean.parquet"
    csv     = PROC_DIR / "sar_incidents_clean.csv"
    if parquet.exists():
        df = pd.read_parquet(parquet)
    elif csv.exists():
        df = pd.read_csv(csv, low_memory=False)
    else:
        console.print("[yellow]No incident data — map will show GIS layers only[/yellow]")
        return pd.DataFrame()

    df = df.dropna(subset=["latitude","longitude"])
    df = df[
        df["latitude"].between(31.0, 37.0) &
        df["longitude"].between(-115.0, -109.0)
    ].copy()
    return df


def load_geojson(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None



def build_map(df: pd.DataFrame) -> folium.Map:
    # Centre on AZ WUI corridor midpoint
    m = folium.Map(
        location=[33.0, -111.5],
        zoom_start=8,
        tiles=None,
        prefer_canvas=True,
    )

    # ── Base tiles ─────────────────────────────────────────────────────────
    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>',
        name="Dark (CARTO)",
        max_zoom=19,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Satellite (Esri)",
        max_zoom=19,
    ).add_to(m)

    # ── GIS layers ─────────────────────────────────────────────────────────

    # Maricopa parks polygons
    maricopa_parks = load_geojson(EXT_DIR / "maricopa_parks.geojson")
    if maricopa_parks:
        fg_parks_m = folium.FeatureGroup(name="Maricopa County Parks", show=True)
        folium.GeoJson(
            maricopa_parks,
            style_function=lambda _: {
                "fillColor": "#7a9e7e",
                "color":     "#7a9e7e",
                "weight":    1,
                "fillOpacity": 0.18,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["ParkName"] if _has_field(maricopa_parks, "ParkName") else [],
                aliases=["Park:"],
                localize=True,
            ),
        ).add_to(fg_parks_m)
        fg_parks_m.add_to(m)
        console.print("  [green]✓[/green] Maricopa parks layer")

    # Phoenix preserves
    phoenix_parks = load_geojson(EXT_DIR / "phoenix_parks.geojson")
    if phoenix_parks:
        fg_phoenix = folium.FeatureGroup(name="Phoenix Mountain Preserves", show=True)
        folium.GeoJson(
            phoenix_parks,
            style_function=lambda _: {
                "fillColor": "#c8a96e",
                "color":     "#c8a96e",
                "weight":    1.5,
                "fillOpacity": 0.22,
            },
        ).add_to(fg_phoenix)
        fg_phoenix.add_to(m)
        console.print("  [green]✓[/green] Phoenix preserves layer")

    # Pima parks
    pima_parks = load_geojson(EXT_DIR / "pima_parks.geojson")
    if pima_parks:
        fg_parks_p = folium.FeatureGroup(name="Pima County Parks", show=True)
        folium.GeoJson(
            pima_parks,
            style_function=lambda _: {
                "fillColor": "#4a8fa8",
                "color":     "#4a8fa8",
                "weight":    1,
                "fillOpacity": 0.18,
            },
        ).add_to(fg_parks_p)
        fg_parks_p.add_to(m)
        console.print("  [green]✓[/green] Pima parks layer")

    # Pima open space
    pima_open = load_geojson(EXT_DIR / "pima_open_space.geojson")
    if pima_open:
        fg_open = folium.FeatureGroup(name="Pima Open Space", show=False)
        folium.GeoJson(
            pima_open,
            style_function=lambda _: {
                "fillColor": "#5ba3c0",
                "color":     "#5ba3c0",
                "weight":    0.8,
                "fillOpacity": 0.12,
            },
        ).add_to(fg_open)
        fg_open.add_to(m)

    # Maricopa trails
    maricopa_trails = load_geojson(EXT_DIR / "maricopa_trails.geojson")
    if maricopa_trails:
        fg_trails = folium.FeatureGroup(name="Maricopa Trails", show=False)
        folium.GeoJson(
            maricopa_trails,
            style_function=lambda _: {
                "color":   "#e8793a",
                "weight":  1.2,
                "opacity": 0.5,
            },
        ).add_to(fg_trails)
        fg_trails.add_to(m)
        console.print("  [green]✓[/green] Maricopa trails layer")

    # Trailheads (combined)
    trailheads = load_geojson(EXT_DIR / "trailheads_combined.geojson")
    if trailheads:
        fg_th = folium.FeatureGroup(name="Trailheads", show=True)
        for feat in trailheads.get("features", []):
            coords = feat.get("geometry", {}).get("coordinates", [])
            if len(coords) < 2:
                continue
            props = feat.get("properties", {})
            name  = props.get("name", "Trailhead")
            folium.CircleMarker(
                location=[coords[1], coords[0]],
                radius=4,
                color="#e8793a",
                fill=True,
                fill_color="#e8793a",
                fill_opacity=0.9,
                weight=1,
                tooltip=name,
            ).add_to(fg_th)
        fg_th.add_to(m)
        console.print(f"  [green]✓[/green] Trailheads layer")

    # ── Incident layers ────────────────────────────────────────────────────
    if not df.empty:

        # Heatmap (all incidents, off by default)
        fg_heat = folium.FeatureGroup(name="🔥 Incident Heatmap", show=False)
        HeatMap(
            df[["latitude","longitude"]].dropna().values.tolist(),
            radius=14, blur=18, max_zoom=13,
            gradient={0.2:"#1a1e18", 0.5:"#e8793a", 0.8:"#d94f1e", 1.0:"#ffffff"},
        ).add_to(fg_heat)
        fg_heat.add_to(m)

        # One CircleMarker FeatureGroup per cluster — colors render correctly
        cluster_col = "behavioral_cluster"
        for cluster_key, color in CLUSTER_COLORS.items():
            label = CLUSTER_LABELS.get(cluster_key, cluster_key)
            sub   = df[df[cluster_col] == cluster_key] if cluster_col in df.columns else pd.DataFrame()
            if sub.empty:
                continue

            fg = folium.FeatureGroup(name=f"⬤ {label}", show=True)
            for _, row in sub.iterrows():
                popup_html = (
                    f"<div style='font-family:monospace;font-size:11px;min-width:190px;"
                    f"line-height:1.6;background:#0d0f0c;color:#d4cbb8;"
                    f"padding:8px;border:1px solid {color};'>"
                    f"<span style='color:{color};font-weight:bold;'>{label}</span><br>"
                    f"<b>{row.get('location_name','')}</b><br>"
                    f"<span style='color:#c8a96e;'>{row.get('incident_type','')}</span><br>"
                    f"📅 {str(row.get('date',''))[:10]} &nbsp;🕐 {row.get('time_of_day_bucket','')}<br>"
                    f"📞 {row.get('caller_context','')}<br>"
                    f"🏃 {row.get('activity_at_onset','')}"
                    f"</div>"
                )
                folium.CircleMarker(
                    location=[row["latitude"], row["longitude"]],
                    radius=5,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.75,
                    weight=1,
                    popup=folium.Popup(popup_html, max_width=230),
                    tooltip=f"{label} — {row.get('incident_type','')}",
                ).add_to(fg)
            fg.add_to(m)

        n_clusters = df[cluster_col].nunique() if cluster_col in df.columns else "?"
        console.print(f"  [green]✓[/green] {len(df):,} incidents · {n_clusters} clusters · CircleMarker (color coded)")

    # ── Legend ─────────────────────────────────────────────────────────────
    legend_html = _build_legend()
    m.get_root().html.add_child(folium.Element(legend_html))

    # ── Layer control ──────────────────────────────────────────────────────
    folium.LayerControl(collapsed=False, position="topright").add_to(m)

    return m


def _has_field(geojson: dict, field: str) -> bool:
    features = geojson.get("features", [])
    if not features:
        return False
    return field in features[0].get("properties", {})



def _build_legend() -> str:
    items = "".join(
        f'<div style="display:flex;align-items:center;gap:6px;margin:3px 0;">'
        f'<div style="width:10px;height:10px;border-radius:50%;background:{color};flex-shrink:0;"></div>'
        f'<span style="font-size:10px;">{label}</span></div>'
        for key, color in CLUSTER_COLORS.items()
        if key != "unknown"
        for label in [CLUSTER_LABELS.get(key, key)]
    )
    return f"""
    <div style="
        position:fixed;bottom:30px;left:12px;z-index:9999;
        background:#0d0f0c;border:1px solid #2e3429;
        padding:10px 14px;font-family:monospace;
        box-shadow:0 2px 8px rgba(0,0,0,0.6);
        max-width:220px;
    ">
      <div style="color:#c8a96e;font-size:10px;letter-spacing:2px;
                  text-transform:uppercase;margin-bottom:8px;font-weight:bold;">
        ▶ RIDGELINE // SAR Clusters
      </div>
      {items}
      <div style="color:#6b6a5e;font-size:9px;margin-top:8px;border-top:1px solid #2e3429;padding-top:6px;">
        bdgroves.github.io/ridgeline
      </div>
    </div>
    """


def main() -> None:
    console.rule("[bold]RIDGELINE — Interactive Map Build[/bold]")

    df = load_incidents()
    console.print(f"  Incidents loaded: [cyan]{len(df):,}[/cyan]")

    ext_files = list(EXT_DIR.glob("*.geojson"))
    console.print(f"  GIS layers available: [cyan]{len(ext_files)}[/cyan]")
    if not ext_files:
        console.print(
            "  [yellow]No GIS data found — run `pixi run gis` first.[/yellow]\n"
            "  Map will render with incident data only."
        )

    m = build_map(df)

    out = SITE_DIR / "map.html"
    m.save(str(out))
    console.print(f"\n  [green]✓[/green] map.html ({out.stat().st_size:,} bytes)")
    console.rule("[green]Map build done[/green]")


if __name__ == "__main__":
    main()
