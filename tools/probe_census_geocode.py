"""
Try the U.S. Census Bureau geocoder (public domain, storage allowed) on every
Phoenix address the Maricopa geocoder couldn't place. Records each match with
the same checks the main geocoder uses (same street words, same direction,
same street type). Writes data/external/census_geocode_probe.json. Runs in CI.
"""
import json, sys
from pathlib import Path
import httpx
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "ingest"))
from addresses import normalize, street_words, direction_conflict, type_conflict  # noqa: E402

cache = pd.read_csv(ROOT / "data" / "external" / "geocode_cache.csv")
calls = pd.read_csv(ROOT / "data" / "external" / "phoenix_mountain_calls.csv")
failed = cache[(cache["method"] == "failed") & ~cache["address"].str.startswith("SCOTTSDALE|")]
n_calls = calls["address"].value_counts()
failed = failed[failed["address"].isin(n_calls.index)]
c = httpx.Client(timeout=60, headers={"User-Agent": "ridgeline (brooksgroves.com)"})
out = []
for addr in failed["address"]:
    q, prec = normalize(addr)
    one = q.replace(" & ", " and ")
    rec = {"address": addr, "query": q, "precision": prec, "calls": int(n_calls.get(addr, 0))}
    try:
        if prec == "intersection":
            rec["skipped"] = "intersection"
        else:
            r = c.get("https://geocoding.geo.census.gov/geocoder/locations/onelineaddress",
                      params={"address": f"{one}, Phoenix, AZ", "benchmark": "Public_AR_Current", "format": "json"})
            m = r.json().get("result", {}).get("addressMatches", [])
            if m:
                best = m[0]
                ma = best["matchedAddress"]
                rec.update(match=ma, lon=best["coordinates"]["x"], lat=best["coordinates"]["y"], n_matches=len(m),
                           same_street=all(ws & set(ma.upper().replace(",", " ").split()) for ws in street_words(q)),
                           dir_conflict=direction_conflict(q, ma), type_conflict=type_conflict(q, ma))
    except Exception as e:
        rec["error"] = repr(e)
    out.append(rec)
ok = [r for r in out if r.get("match") and r["same_street"] and not r["dir_conflict"] and not r["type_conflict"]]
rep = {"tried": len(out), "matched_ok": len(ok), "calls_ok": sum(r["calls"] for r in ok),
       "calls_tried": sum(r["calls"] for r in out), "results": out}
(ROOT / "data" / "external" / "census_geocode_probe.json").write_text(json.dumps(rep, indent=1))
print({k: v for k, v in rep.items() if k != "results"})
