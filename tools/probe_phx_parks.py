"""
City of Phoenix Parks open-data map service: list its layers and save any
trailhead / access-point layer as GeoJSON (EPSG:4326). Runs in CI.
Writes data/external/phoenix_parks_probe.json and phoenix_city_trailheads.geojson.
"""
import json, re
from pathlib import Path
import httpx

EXT = Path(__file__).resolve().parents[1] / "data" / "external"
SVC = "https://maps.phoenix.gov/pub/rest/services/Public/ParksOpenData/MapServer"
c = httpx.Client(timeout=120, headers={"User-Agent": "ridgeline (brooksgroves.com)"})
rep = {"service": SVC}
info = c.get(SVC, params={"f": "json"}).json()
rep["layers"] = [(l["id"], l["name"]) for l in info.get("layers", [])]
feats_all = []
for lid, name in rep["layers"]:
    if not re.search(r"trail\s*head|trailhead|access|parking", name, re.I):
        continue
    li = c.get(f"{SVC}/{lid}", params={"f": "json"}).json()
    rep[f"layer:{lid}"] = {"name": name, "geometryType": li.get("geometryType"),
                           "fields": [f["name"] for f in li.get("fields", [])]}
    q = c.get(f"{SVC}/{lid}/query", params={"where": "1=1", "outFields": "*", "outSR": 4326,
                                             "returnGeometry": "true", "f": "geojson"}).json()
    fs = q.get("features", [])
    for f in fs:
        f["properties"]["_layer"] = name
    feats_all += fs
    rep[f"layer:{lid}"]["count"] = len(fs)
    rep[f"layer:{lid}"]["sample"] = [f["properties"] for f in fs[:3]]
(EXT / "phoenix_city_trailheads.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": feats_all}))
(EXT / "phoenix_parks_probe.json").write_text(json.dumps(rep, indent=1, default=str))
print(json.dumps(rep, indent=1, default=str)[:4000])
