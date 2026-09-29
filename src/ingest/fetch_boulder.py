"""
ridgeline / src / ingest / fetch_boulder.py

Boulder Fire-Rescue incidents → technical and water rescues.

Source: City of Boulder Open Data, "Response Times For Boulder Fire-Rescue"
        maps.bouldercolorado.gov/.../FireResponseTimesOpenData/MapServer/0
        One row per incident, 2015 → present, point geometry, CC0.

Boulder publishes a location for every incident, so nothing is geocoded.
Rescue calls are identified from CALLTYPE and, where present, the NFIRS
incident description (FHINCIDENTDESCRIPTION). "Auto-Aid" calls are Boulder
units responding outside the city, often into county open space.

Outputs:
    data/external/boulder_sar_incidents.geojson
    data/external/boulder_report.json

Run:
    pixi run boulder
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from rich.console import Console

from arcgis import EXT_DIR, query_all, write_city_geojson

LAYER = "https://maps.bouldercolorado.gov/arcgis/rest/services/fire/FireResponseTimesOpenData/MapServer/0"

MOUNTAIN_CALLTYPES = ("Rescue", "Auto-Aid Tech Rescue", "Auto-Aid Rescue")
WATER_CALLTYPES    = ("Water Rescue", "Auto-Aid Water Rescue")
MOUNTAIN_NFIRS = ("High-angle rescue", "Search for person on land", "Search for lost person, other")
WATER_NFIRS    = ("Swift water rescue", "Search for person in water", "Ice rescue",
                  "Water & ice-related rescue, other", "Swimming/recreational water areas rescue")
# Rescue-typed calls whose NFIRS code shows they weren't backcountry work.
NOT_MOUNTAIN_NFIRS = ("Extrication, rescue, Other", "Trench/below-grade rescue",
                      "Confined space rescue", "Animal rescue")

def _in(field, values):
    return f"{field} IN (" + ",".join("'" + v.replace("'", "''") + "'" for v in values) + ")"

WHERE = " OR ".join([_in("CALLTYPE", MOUNTAIN_CALLTYPES + WATER_CALLTYPES),
                     _in("FHINCIDENTDESCRIPTION", MOUNTAIN_NFIRS + WATER_NFIRS)])
FIELDS = "MASTERINCIDENTNUMBER,RESPONSEDATE,CALLTYPE,PROBLEM,FHINCIDENTDESCRIPTION"
TZ = ZoneInfo("America/Denver")

console = Console()


def category(calltype: str, nfirs: str) -> str | None:
    if nfirs in WATER_NFIRS or calltype in WATER_CALLTYPES:
        return "water"
    if nfirs in NOT_MOUNTAIN_NFIRS:
        return None
    return "mountain"


def main() -> None:
    console.rule("[bold]RIDGELINE — Boulder Fire-Rescue[/bold]")
    feats = query_all(LAYER, WHERE, out_fields=FIELDS, geometry=True,
                      client=httpx.Client(timeout=90, follow_redirects=True))
    console.print(f"  Rows returned: [cyan]{len(feats):,}[/cyan]")

    out, seen, dropped = [], set(), Counter()
    for f in feats:
        a, g = f["attributes"], f.get("geometry") or {}
        key = a.get("MASTERINCIDENTNUMBER")
        if key in seen:
            continue
        seen.add(key)
        cat = category(a.get("CALLTYPE") or "", a.get("FHINCIDENTDESCRIPTION") or "")
        if cat is None:
            dropped[a.get("FHINCIDENTDESCRIPTION")] += 1
            continue
        ts = a.get("RESPONSEDATE")
        dt = datetime.fromtimestamp(ts / 1000, tz=TZ) if ts else None
        out.append({
            "incident_id":   key,
            "incident_type": a.get("FHINCIDENTDESCRIPTION") or a.get("CALLTYPE"),
            "call_type":     a.get("CALLTYPE"),
            "category":      cat,
            "date":          dt.date().isoformat() if dt else None,
            "year":          dt.year if dt else None,
            "month":         dt.month if dt else None,
            "hour":          dt.hour if dt else None,
            "weekday":       dt.weekday() if dt else None,
            "is_weekend":    (dt.weekday() >= 5) if dt else None,
            "location_name": "",                     # Boulder publishes points, not addresses
            "precision":     "published_point",
            "latitude":      g.get("y"),
            "longitude":     g.get("x"),
        })

    path = write_city_geojson("boulder", out)
    dates = sorted(r["date"] for r in out if r["date"])
    report = {
        "source": LAYER,
        "incidents": len(out),
        "located": sum(1 for r in out if r["latitude"] is not None),
        "distinct_points": len({(r["latitude"], r["longitude"]) for r in out}),
        "by_category": dict(Counter(r["category"] for r in out)),
        "by_call_type": dict(Counter(r["call_type"] for r in out)),
        "by_nfirs": dict(Counter(str(r["incident_type"]) for r in out)),
        "excluded_non_backcountry": dict(dropped),
        "date_range": [dates[0], dates[-1]] if dates else None,
    }
    (EXT_DIR / "boulder_report.json").write_text(json.dumps(report, indent=2))
    console.print(f"  [green]✓[/green] {report['located']:,} of {report['incidents']:,} located → {path.name}")


if __name__ == "__main__":
    main()
