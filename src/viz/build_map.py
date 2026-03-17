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
    "recreational_underequipped":   "#e8793a",
    "flash_flood_stranded":         "#4a9fd4",
    "unhoused_encampment":          "#95a5a6",
    "wildland_fire":                "#d94f1e",
    "dog_walker":                   "#f5a623",
    "casual_proximity":             "#7ed321",
    "party_social_spillover":       "#c8a96e",
    "youth_teen":                   "#9b59b6",
    "unknown":                      "#555555",
}

CLUSTER_LABELS = {
    "recreational_underequipped":   "Recreational - Underequipped",
    "flash_flood_stranded":         "Flash Flood Stranded",
    "unhoused_encampment":          "Crisis / Behavioral Health",
    "wildland_fire":                "Wildland Fire",
    "dog_walker":                   "Dog Walker",
    "casual_proximity":             "Casual Proximity",
    "party_social_spillover":       "Party / Social Spillover",
    "youth_teen":                   "Youth / Teen",
    "unknown":                      "Unknown",
}


def load_incidents() -> pd.DataFrame:
    """
    Load SAR incidents from pre-exported GeoJSON (tracked in git).
    Falls back to geocoded parquet if available locally.
    GeoJSON ensures production map always has incident dots.
    """
    import json

    # ── Primary: pre-exported GeoJSON (tracked in git, works in CI) ───────
    geojson_path = EXT_DIR / "phoenix_sar_incidents.geojson"
    if geojson_path.exists():
        with open(geojson_path) as f:
            gj = json.load(f)
        rows = []
        for feat in gj.get("features", []):
            props = feat.get("properties", {})
            coords = feat.get("geometry", {}).get("coordinates", [None, None])
            rows.append({**props,
                         "longitude": coords[0],
                         "latitude":  coords[1]})
        df = pd.DataFrame(rows)
        console.print(f"  [green]✓[/green] Phoenix: [cyan]{len(df):,}[/cyan] SAR incidents (GeoJSON)")
        return df

    # ── Fallback: geocoded parquet (local only) ────────────────────────────
    phx_geo = PROC_DIR / "phoenix_fire_sar_geocoded.parquet"
    if phx_geo.exists():
        df = pd.read_parquet(phx_geo)
        if "behavioral_cluster" not in df.columns:
            df["behavioral_cluster"] = "recreational_underequipped"
        incident = df["incident_type"].fillna("").str.lower()
        SAR_STRICT = ["mountain rescue","water rescue","swift water","technical rescue",
                      "heat exhaustion","heat stroke","lost person",
                      "check flooding condition","stranded"]
        mask = pd.Series(False, index=df.index)
        for term in SAR_STRICT:
            mask |= incident.str.contains(term, na=False)
        df = df[mask].dropna(subset=["latitude","longitude"]).copy()
        console.print(f"  [green]✓[/green] Phoenix: [cyan]{len(df):,}[/cyan] SAR incidents (parquet)")
        return df

    console.print("[yellow]No incident data — map will show GIS layers only[/yellow]")
    return pd.DataFrame()

    combined = pd.DataFrame()  # unreachable — keeps linter happy
    combined = combined[
        combined["latitude"].between(30.0, 42.0) &
        combined["longitude"].between(-120.0, -109.0)
    ].copy()




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
        location=[33.52, -112.00],   # Phoenix WUI core — Camelback/South Mtn
        zoom_start=11,
        tiles=None,
        prefer_canvas=True,
    )

    # ── Base tiles — dark first so it's the default ────────────────────────
    # Dark CARTO — default (first tile layer = active on load)
    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>',
        name="Dark (CARTO)",
        max_zoom=19,
    ).add_to(m)

    # OpenStreetMap
    folium.TileLayer(
        tiles="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        name="OpenStreetMap",
        max_zoom=19,
    ).add_to(m)

    # Satellite
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
        fg_parks_m = folium.FeatureGroup(name="Maricopa County Parks", show=False)
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
        fg_open = folium.FeatureGroup(name="Pima Open Space (coming soon)", show=False)
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
            props  = feat.get("properties", {})
            name   = props.get("name", "Trailhead")
            county = props.get("county", "")
            # Phoenix only for now — skip Pima trailheads until Tucson data lands
            if str(county).strip().title() == "Pima":
                continue
            # Only show if within Maricopa bbox (lat 33.0-34.2, lon -113.5 to -111.0)
            lat, lon = coords[1], coords[0]
            if not (33.0 <= lat <= 34.2 and -113.5 <= lon <= -111.0):
                continue
            folium.CircleMarker(
                location=[lat, lon],
                radius=3,
                color="#c8a96e",
                fill=True,
                fill_color="#c8a96e",
                fill_opacity=0.7,
                weight=0,
                tooltip=f"🏔 {name}",
                # No popup — keeps clicks clean for incident dots
            ).add_to(fg_th)
        fg_th.add_to(m)
        console.print(f"  [green]✓[/green] Trailheads layer (Maricopa only)")

    # ── Incident layers ────────────────────────────────────────────────────
    if not df.empty:

        # Heatmap (all incidents, off by default)
        fg_heat = folium.FeatureGroup(name="Incident Heatmap", show=False)
        HeatMap(
            df[["latitude","longitude"]].dropna().values.tolist(),
            radius=14, blur=18, max_zoom=13,
            gradient={0.2:"#1a1e18", 0.5:"#e8793a", 0.8:"#d94f1e", 1.0:"#ffffff"},
        ).add_to(fg_heat)
        fg_heat.add_to(m)

        # One CircleMarker FeatureGroup per cluster — colors render correctly
        cluster_col = "behavioral_cluster"
        if cluster_col in df.columns:
            console.print(f"  Cluster distribution: {df[cluster_col].value_counts().to_dict()}")
        for cluster_key, color in CLUSTER_COLORS.items():
            label = CLUSTER_LABELS.get(cluster_key, cluster_key)
            sub   = df[df[cluster_col] == cluster_key] if cluster_col in df.columns else pd.DataFrame()
            if sub.empty:
                continue

            fg = folium.FeatureGroup(name=label, show=True)
            for _, row in sub.iterrows():
                # ── Sitrep fields ──────────────────────────────────────────
                date_str     = str(row.get("date", ""))[:10]
                hour         = row.get("hour", None)
                hour_str     = f"{int(hour):02d}:00" if pd.notna(hour) else "—"
                tod          = str(row.get("time_of_day_bucket", "")).upper()
                location     = str(row.get("location_name", "")).strip()
                inc_type     = str(row.get("incident_type", "")).upper()
                cluster_lbl  = label.upper()
                year         = str(row.get("year", ""))
                weekend      = row.get("is_weekend", False)
                dow          = "WEEKEND" if weekend else "WEEKDAY"
                MONTHS = ["","JAN","FEB","MAR","APR","MAY","JUN",
                           "JUL","AUG","SEP","OCT","NOV","DEC"]
                # Derive month from date string — always available
                try:
                    month_str = MONTHS[int(str(row.get("date",""))[:7].split("-")[1])]
                except (ValueError, IndexError, AttributeError):
                    month_num = row.get("month", None)
                    try:
                        month_str = MONTHS[int(float(month_num))]
                    except Exception:
                        month_str = "—"

                popup_html = f"""
<div style='font-family:monospace;font-size:10px;width:260px;
            line-height:1.7;background:#0d0f0c;color:#d4cbb8;
            padding:10px 12px;border:1px solid {color};
            box-shadow:0 2px 8px rgba(0,0,0,0.8);'>

  <div style='color:{color};font-size:9px;letter-spacing:2px;
              margin-bottom:6px;border-bottom:1px solid #2e3429;padding-bottom:4px;'>
    ▶ RIDGELINE // SITREP
  </div>

  <div style='color:#c8a96e;font-weight:bold;font-size:11px;margin-bottom:2px;'>
    {inc_type}
  </div>

  <div style='color:#d4cbb8;margin-bottom:6px;'>
    {location}
  </div>

<div style='border-top:1px solid #2e3429;margin-top:6px;padding-top:6px;'><div style='color:#6b6a5e;font-size:9px;'>DATE &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; TIME</div><div style='color:{color};margin-bottom:4px;'>{date_str} &nbsp;&nbsp; {hour_str} {tod}</div><div style='color:#6b6a5e;font-size:9px;'>DOW &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; MONTH</div><div style='color:#d4cbb8;margin-bottom:4px;'>{dow} &nbsp;&nbsp; {month_str} {year}</div><div style='color:#6b6a5e;font-size:9px;'>CLUSTER</div><div style='color:{color};font-size:9px;margin-bottom:2px;'>{cluster_lbl}</div><div style='color:#6b6a5e;font-size:9px;'>CITY</div><div style='color:#d4cbb8;font-size:9px;'>PHOENIX · AZ</div></div>

  <div style='color:#6b6a5e;font-size:8px;margin-top:6px;
              border-top:1px solid #2e3429;padding-top:4px;'>
    SOURCE: PHOENIX FIRE DEPT OPEN DATA
  </div>
</div>"""
                # Small jitter so incidents don't stack exactly on trailhead pins
                import random as _rnd
                lat_j = row["latitude"]  + _rnd.gauss(0, 0.003)
                lon_j = row["longitude"] + _rnd.gauss(0, 0.003)
                folium.CircleMarker(
                    location=[lat_j, lon_j],
                    radius=9,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.88,
                    weight=1.5,
                    popup=folium.Popup(popup_html, max_width=280),
                    tooltip=f"{label} — {row.get('incident_type','')}",
                ).add_to(fg)
            fg.add_to(m)

        n_clusters = df[cluster_col].nunique() if cluster_col in df.columns else "?"
        console.print(f"  [green]✓[/green] {len(df):,} incidents · {n_clusters} clusters · CircleMarker (color coded)")

    # ── Legend ─────────────────────────────────────────────────────────────
    legend_html = _build_legend()
    m.get_root().html.add_child(folium.Element(legend_html))

    # ── Layer control — collapsed by default, scrollable when open ─────────
    folium.LayerControl(collapsed=False, position="topright").add_to(m)
    m.get_root().header.add_child(folium.Element(
        "<style>"
        ".leaflet-control-layers{"
        "max-height:calc(100vh - 100px) !important;"
        "overflow-y:auto !important;"
        "scrollbar-width:thin;"
        "}"
        "</style>"
    ))

    return m


def _has_field(geojson: dict, field: str) -> bool:
    features = geojson.get("features", [])
    if not features:
        return False
    return field in features[0].get("properties", {})



def _build_legend() -> str:
    items = "".join(
        f'<div style="display:flex;align-items:center;gap:8px;margin:4px 0;">'
        f'<div style="width:14px;height:14px;border-radius:50%;background:{color};'
        f'flex-shrink:0;border:2px solid rgba(255,255,255,0.3);"></div>'
        f'<span style="font-size:11px;color:#d4cbb8;">{label}</span></div>'
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
    # Save with explicit UTF-8 to prevent Windows cp1252 encoding drops
    html_content = m.get_root().render()
    out.write_text(html_content, encoding="utf-8")
    console.print(f"\n  [green]✓[/green] map.html ({out.stat().st_size:,} bytes)")
    console.rule("[green]Map build done[/green]")


if __name__ == "__main__":
    main()
