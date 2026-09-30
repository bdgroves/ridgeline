"""
ridgeline / src / ingest / fetch_trail_counts.py

City of Phoenix "Hiking Trail Usage" open data: infrared trail-counter counts
at Phoenix hiking sites, January 2019 on (CC BY). Saved as-is to
data/external/phoenix_trail_counts.csv, with a summary of what it contains in
data/external/trail_counts_report.json. Falls back to the committed copy.

https://www.phoenixopendata.com/dataset/hiking-trail-usage
"""
from __future__ import annotations

import csv
import io
import json
from collections import Counter
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[2]
EXT = ROOT / "data" / "external"
OUT = EXT / "phoenix_trail_counts.csv"
REPORT = EXT / "trail_counts_report.json"
URL = ("https://www.phoenixopendata.com/dataset/1f46604c-ce46-4503-8e9b-45b498456185/"
       "resource/aa4e2a08-c0ad-4fc4-bee9-44c2d85a58fa/download/hiking.csv")


def main() -> None:
    try:
        r = httpx.get(URL, timeout=120, follow_redirects=True,
                      headers={"User-Agent": "ridgeline (brooksgroves.com)"})
        r.raise_for_status()
        text = r.content.decode("utf-8-sig", errors="replace")
        tmp = OUT.with_suffix(".tmp"); tmp.write_text(text); tmp.replace(OUT)
    except Exception as e:
        print(f"  trail counts: using committed copy ({e!r})")
    rows = list(csv.DictReader(io.StringIO(OUT.read_text())))
    cols = list(rows[0].keys()) if rows else []
    rep = {"url": URL, "rows": len(rows), "columns": cols, "sample": rows[:5]}
    for c in cols:
        vc = Counter(r[c] for r in rows)
        if len(vc) <= 80:
            rep[f"values:{c}"] = vc.most_common(80)
        else:
            vals = sorted(v for v in vc if v)
            rep[f"range:{c}"] = [vals[0], vals[-1]] if vals else None
    REPORT.write_text(json.dumps(rep, indent=1))
    print(json.dumps({k: v for k, v in rep.items() if k != "sample"}, indent=1)[:3000])


if __name__ == "__main__":
    main()
