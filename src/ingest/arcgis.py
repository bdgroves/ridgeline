"""
ridgeline / src / ingest / arcgis.py

Small helpers for pulling rows out of an ArcGIS REST layer and writing the
per-city incident files every city shares.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx

ROOT    = Path(__file__).resolve().parents[2]
EXT_DIR = ROOT / "data" / "external"


def query_all(layer_url: str, where: str, out_fields: str = "*",
              geometry: bool = False, client: httpx.Client | None = None,
              chunk: int = 500) -> list[dict]:
    """
    Every feature matching `where`. Asks for object IDs first (no server
    record limit on that call), then fetches in chunks, so it works whether
    or not the layer supports pagination.
    """
    c = client or httpx.Client(timeout=90, follow_redirects=True)
    ids = c.get(f"{layer_url}/query", params={
        "where": where, "returnIdsOnly": "true", "f": "json"}).json()
    oids = sorted(ids.get("objectIds") or [])
    feats: list[dict] = []
    for i in range(0, len(oids), chunk):
        batch = oids[i:i + chunk]
        r = c.post(f"{layer_url}/query", data={
            "objectIds": ",".join(map(str, batch)),
            "outFields": out_fields,
            "returnGeometry": "true" if geometry else "false",
            "outSR": "4326",
            "f": "json",
        })
        r.raise_for_status()
        feats.extend(r.json().get("features", []))
    return feats


def write_city_geojson(city: str, rows: list[dict]) -> Path:
    """
    rows: dicts with latitude, longitude and the shared properties
    (incident_type, category, date, year, month, hour, weekday, is_weekend,
    location_name, precision). Rows without coordinates are skipped.
    """
    feats = []
    for r in rows:
        if r.get("latitude") is None or r.get("longitude") is None:
            continue
        props = {k: v for k, v in r.items() if k not in ("latitude", "longitude")}
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point",
                         "coordinates": [round(float(r["longitude"]), 6),
                                         round(float(r["latitude"]), 6)]},
            "properties": props,
        })
    out = EXT_DIR / f"{city}_sar_incidents.geojson"
    out.write_text(json.dumps({"type": "FeatureCollection", "features": feats},
                              separators=(",", ":"), default=str))
    return out
