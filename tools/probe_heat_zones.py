"""
Compare NWS heat warning (and watch) days across the Phoenix-area forecast
zones, 2021-2024, against the City of Phoenix program review (20, 18, 42, 45
warning days). Also keeps the raw VTEC rows for 2021-2022, including any
upgraded (UPG) watches. Writes data/external/probe_heat_zones.json. Runs in CI.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import httpx

OUT = Path(__file__).resolve().parents[1] / "data" / "external" / "probe_heat_zones.json"
MST = timezone(timedelta(hours=-7))
ZONES = {"AZZ543": "Central Phoenix", "AZZ546": "Scottsdale/Paradise Valley",
         "AZZ544": "North Phoenix/Glendale", "AZZ542": "Deer Valley", "AZZ548": "East Valley",
         "AZZ540": "?", "AZZ541": "?", "AZZ547": "?", "AZZ550": "?", "AZZ551": "Southeast Valley"}
c = httpx.Client(timeout=90, headers={"User-Agent": "ridgeline (brooksgroves.com)"})


def days(rows, lo=0, hi=24):
    out = set()
    for e in rows:
        a = datetime.fromisoformat(e["issue"].replace("Z", "+00:00")).astimezone(MST)
        b = datetime.fromisoformat(e["expire"].replace("Z", "+00:00")).astimezone(MST)
        if b < a:
            continue
        d = a.date()
        while d <= b.date():
            s = datetime(d.year, d.month, d.day, lo, tzinfo=MST); t = datetime(d.year, d.month, d.day, hi, tzinfo=MST) if hi < 24 else datetime(d.year, d.month, d.day, tzinfo=MST) + timedelta(days=1)
            if a < t and b > s:
                out.add(d)
            d += timedelta(days=1)
    return out


rep = {"city_review": {2021: 20, 2022: 18, 2023: 42, 2024: 45}, "zones": {}}
allw = {}
for z, name in ZONES.items():
    try:
        ev = c.get("https://mesonet.agron.iastate.edu/json/vtec_events_byugc.php",
                   params={"ugc": z, "sdate": "2021-01-01", "edate": "2025-01-01"}).json().get("events", [])
    except Exception as e:
        rep["zones"][z] = {"error": repr(e)}; continue
    heat = [e for e in ev if e.get("phenomena") in ("EH", "XH")]
    W = [e for e in heat if e.get("significance") == "W"]
    A = [e for e in heat if e.get("significance") == "A"]
    r = {"name": name}
    for label, rows, lo, hi in [("warn_any_hour", W, 0, 24), ("warn_9to5", W, 9, 17),
                                ("watch_any_hour", A, 0, 24), ("watch_or_warn_any", W + A, 0, 24)]:
        ds = days(rows, lo, hi)
        r[label] = {y: sum(1 for d in ds if d.year == y) for y in (2021, 2022, 2023, 2024)}
        if label == "warn_any_hour":
            allw[z] = ds
    r["rows_2021_2022"] = [e for e in heat if e["issue"][:4] in ("2021", "2022") or e["expire"][:4] in ("2021", "2022")]
    r["expire_before_issue"] = [e for e in heat if e["expire"] < e["issue"]]
    rep["zones"][z] = r
u = set().union(*allw.values()) if allw else set()
rep["union_warn_any_hour"] = {y: sum(1 for d in u if d.year == y) for y in (2021, 2022, 2023, 2024)}
if "AZZ543" in allw:
    rep["days_2022_in_other_zones_not_543"] = sorted(str(d) for d in u - allw["AZZ543"] if d.year == 2022)
# IEM's per-event API, to see how the Aug 30 - Sep 7, 2022 event is recorded
try:
    rep["event_2022_EH_W_0006"] = c.get("https://mesonet.agron.iastate.edu/json/vtec_event.py",
        params={"wfo": "PSR", "year": 2022, "phenomena": "EH", "significance": "W", "etn": 6}).json()
except Exception as e:
    rep["event_2022_error"] = repr(e)
OUT.write_text(json.dumps(rep, indent=1, default=str))
print(json.dumps({k: v for k, v in rep.items() if k != "zones"}, indent=1, default=str)[:3000])
for z, r in rep["zones"].items():
    print(z, r.get("name"), {k: r.get(k) for k in ("warn_any_hour", "warn_9to5", "watch_any_hour")})
