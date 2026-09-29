"""
ridgeline / src / ingest / fetch_scottsdale.py

Scottsdale Fire Department calls for service → mountain and water rescues.

Source: City of Scottsdale Open Data, "Fire Department Calls for Service"
        maps.scottsdaleaz.gov/.../OpenData_Tabular/MapServer/20
        Completed incidents, updated weekly, rolling history from Dec 2022.

Scottsdale codes mountain rescue directly (TypeGroup MTNRES), and publishes
street addresses rather than hundred blocks. There are no coordinates, so
addresses go through the same Maricopa County geocoder as Phoenix, sharing
its cache. The timestamp is a date only, so there is no hour of day.

Outputs:
    data/external/scottsdale_sar_incidents.geojson
    data/external/scottsdale_report.json

Run:
    pixi run scottsdale
"""

from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime, timezone

import httpx
from rich.console import Console

from arcgis import EXT_DIR, query_all, write_city_geojson
from geocode_phoenix import geocode_address, load_cache, save_cache

LAYER = "https://maps.scottsdaleaz.gov/arcgis/rest/services/OpenData_Tabular/MapServer/20"
WHERE = ("TypeGroup IN ('MTNRES','WATER') "
         "OR Description LIKE '%Search for person on land%' "
         "OR Description LIKE '%High-angle rescue%' "
         "OR Description LIKE '%Swift water rescue%'")

console = Console()


def category(type_group: str, desc: str) -> str:
    t, d = (type_group or "").strip().upper(), (desc or "").lower()
    if t == "WATER" or "swift water" in d:
        return "water"
    return "mountain"


def main() -> None:
    console.rule("[bold]RIDGELINE — Scottsdale Fire[/bold]")
    client = httpx.Client(timeout=90, follow_redirects=True)
    feats = query_all(LAYER, WHERE, client=client)
    console.print(f"  Rows returned: [cyan]{len(feats):,}[/cyan]")

    # One row per responding unit in some cases — keep one per incident.
    seen, rows = set(), []
    for f in feats:
        a = f["attributes"]
        key = (a.get("IncidentNumber") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        rows.append(a)
    console.print(f"  Unique incidents: [cyan]{len(rows):,}[/cyan]")

    cache = load_cache()
    out, methods = [], Counter()
    for a in rows:
        addr = " ".join(str(a.get("IncidentAddress") or "").split())
        key = f"SCOTTSDALE|{addr}"
        if key not in cache:
            g = geocode_address(addr, client, city="Scottsdale")
            g["address"] = key
            cache[key] = g
            time.sleep(0.05)
        g = cache[key]
        m = str(g.get("method"))
        methods["preserve_centroid" if m.startswith("preserve:") else m] += 1

        ts = a.get("IncidentDate")
        d = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date() if ts else None
        lat, lon = g.get("latitude"), g.get("longitude")
        out.append({
            "incident_id":   a.get("IncidentNumber"),
            "incident_type": " ".join(str(a.get("Description") or "").split()),
            "type_group":    (a.get("TypeGroup") or "").strip(),
            "category":      category(a.get("TypeGroup"), a.get("Description")),
            "date":          d.isoformat() if d else None,
            "year":          d.year if d else None,
            "month":         d.month if d else None,
            "hour":          None,                      # date-only source
            "weekday":       d.weekday() if d else None,
            "is_weekend":    (d.weekday() >= 5) if d else None,
            "location_name": addr,
            "precision":     g.get("precision") if isinstance(g.get("precision"), str) else "",
            "latitude":      None if lat != lat else lat,   # NaN from cache → None
            "longitude":     None if lon != lon else lon,
        })
    save_cache(cache)

    path = write_city_geojson("scottsdale", out)
    dates = sorted(r["date"] for r in out if r["date"])
    report = {
        "source": LAYER,
        "incidents": len(out),
        "located": sum(1 for r in out if r["latitude"] is not None),
        "by_category": dict(Counter(r["category"] for r in out)),
        "by_type_group": dict(Counter(r["type_group"] for r in out)),
        "by_method": dict(methods),
        "date_range": [dates[0], dates[-1]] if dates else None,
    }
    (EXT_DIR / "scottsdale_report.json").write_text(json.dumps(report, indent=2))
    console.print(f"  [green]✓[/green] {report['located']:,} of {report['incidents']:,} located → {path.name}")


if __name__ == "__main__":
    main()
