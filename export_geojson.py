"""
Export strict SAR incidents from geocoded parquet to GeoJSON.
Output tracked in git → available in CI → map has incident dots in production.

Run: pixi run python export_geojson.py
"""
import json
import pandas as pd
from pathlib import Path

ROOT     = Path(__file__).resolve().parent
PROC_DIR = ROOT / "data" / "processed"
EXT_DIR  = ROOT / "data" / "external"

df = pd.read_parquet(PROC_DIR / "phoenix_fire_sar_geocoded.parquet")

SAR_STRICT = [
    "mountain rescue", "water rescue", "swift water",
    "technical rescue", "heat exhaustion", "heat stroke",
    "lost person", "check flooding condition", "stranded",
]
incident = df["incident_type"].fillna("").str.lower()
mask = pd.Series(False, index=df.index)
for term in SAR_STRICT:
    mask |= incident.str.contains(term, na=False)

sar = df[mask].dropna(subset=["latitude", "longitude"]).copy()

# Normalise date and derive month/year if missing
if "date" not in sar.columns and "datetime" in sar.columns:
    sar["date"] = pd.to_datetime(sar["datetime"], errors="coerce").dt.date.astype(str)

dt = pd.to_datetime(sar.get("date"), errors="coerce")
if "month" not in sar.columns or sar["month"].isna().all():
    sar["month"] = dt.dt.month
if "year" not in sar.columns or sar["year"].isna().all():
    sar["year"] = dt.dt.year
# Fill any remaining nulls from date column
sar["month"] = sar["month"].fillna(dt.dt.month)
sar["year"]  = sar["year"].fillna(dt.dt.year)

features = []
for _, row in sar.iterrows():
    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [round(float(row["longitude"]), 6),
                            round(float(row["latitude"]),  6)],
        },
        "properties": {
            "incident_type":       str(row.get("incident_type", "")),
            "behavioral_cluster":  str(row.get("behavioral_cluster", "")),
            "date":                str(row.get("date", "")),
            "hour":                int(row["hour"]) if pd.notna(row.get("hour")) else None,
            "time_of_day_bucket":  str(row.get("time_of_day_bucket", "")),
            "is_weekend":          bool(row.get("is_weekend", False)),
            "location_name":       str(row.get("location_name", "")),
            "year":                int(row["year"]) if pd.notna(row.get("year")) else None,
        },
    })

geojson = {"type": "FeatureCollection", "features": features}

out = EXT_DIR / "phoenix_sar_incidents.geojson"
with open(out, "w") as f:
    json.dump(geojson, f, separators=(",", ":"))

size_kb = out.stat().st_size / 1024
print(f"✓ {len(features):,} incidents → {out.name} ({size_kb:.0f} KB)")
print(f"  incident types: {sar['incident_type'].value_counts().head(5).to_dict()}")
