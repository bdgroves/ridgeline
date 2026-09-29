"""
ridgeline / src / ingest / fetch_nps.py

National Park Service search-and-rescue incidents, 2013-2020.

The NPS FOIA reading room publishes two spreadsheets of SAR incidents
(nps.gov/aboutus/foia/foia-frd.htm). Each row is an incident number, date,
incident type, park code and region. There is no location inside the park,
no time of day and no outcome, so parks are mapped as one marker each.

Also fetched:
  * park boundaries (NPS Land Resources Division service), for a marker point
    and an outline per park,
  * annual recreation visits (NPS IRMA stats), for incidents per million visits.
Each source falls back to the committed copy if the network or service fails.

Writes:
  data/external/nps_sar_incidents.csv   id, date, type, park, region
  data/external/nps_parks.geojson       one polygon per park with incidents (+ centroid)
  data/external/nps_visitation.csv      park, year, visits
  data/external/nps_report.json         what was fetched, from where, and any failures
"""

from __future__ import annotations

import csv
import io
import json
import re
from collections import Counter
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[2]
EXT = ROOT / "data" / "external"
INC = EXT / "nps_sar_incidents.csv"
PARKS = EXT / "nps_parks.geojson"
VISITS = EXT / "nps_visitation.csv"
REPORT = EXT / "nps_report.json"

FILES = [
    "https://www.nps.gov/aboutus/foia/upload/NPS-SAR-Incidents-List-2013-2018.xlsx",
    "https://www.nps.gov/aboutus/foia/upload/SAR-Incidents-List-2019-2020.xlsx",
]
BOUNDARY_SERVICES = [
    "https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/NPS_Land_Resources_Division_Boundary_and_Tract_Data_Service/FeatureServer/2",
    "https://mapservices.nps.gov/arcgis/rest/services/LandResourcesDivisionTractAndBoundaryService/MapServer/2",
]
MORTALITY = ("https://www.nps.gov/aboutus/foia/upload/"
             "FOIA-FAQ-NPS-Mortality-Data-CY2007-to-CY2023-Released-August-2023.xlsx")

c = httpx.Client(timeout=180, follow_redirects=True,
                 headers={"User-Agent": "ridgeline (brooksgroves.com)"})
report: dict = {}


def xlsx_rows(content: bytes) -> list[list[str]]:
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    for ws in wb.worksheets:
        rows = [["" if v is None else str(v).strip() for v in r] for r in ws.iter_rows(values_only=True)]
        for i, r in enumerate(rows):
            if "IncidentNum" in r:
                return [rows[i]] + [x for x in rows[i + 1:] if any(x)]
    return []


def incidents() -> None:
    seen, out, per_file = set(), [], {}
    for url in FILES:
        r = c.get(url); r.raise_for_status()
        rows = xlsx_rows(r.content)
        head, body = rows[0], rows[1:]
        ix = {k: head.index(k) for k in ("IncidentNum", "IncidentDate", "IncType", "ParkAlphaCode", "Region")}
        n = dup = 0
        for row in body:
            iid = row[ix["IncidentNum"]]
            if not iid or iid in seen:
                dup += bool(iid)
                continue
            seen.add(iid)
            m, d, y = (row[ix["IncidentDate"]].split(" ")[0].split("/") + ["", "", ""])[:3]
            if not y:
                continue
            out.append({"id": iid, "date": f"{y}-{int(m):02d}-{int(d):02d}",
                        "type": row[ix["IncType"]], "park": row[ix["ParkAlphaCode"]].upper(),
                        "region": row[ix["Region"]]})
            n += 1
        per_file[url.rsplit("/", 1)[-1]] = {"rows": len(body), "kept": n, "duplicate_ids": dup}
    out.sort(key=lambda r: (r["date"], r["id"]))
    tmp = INC.with_suffix(".tmp")
    with tmp.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "date", "type", "park", "region"])
        w.writeheader(); w.writerows(out)
    tmp.replace(INC)
    report["incidents"] = {"files": per_file, "total": len(out),
                           "no_park_code": sum(1 for r in out if not r["park"])}


def boundaries(codes: set[str]) -> None:
    last = None
    for svc in BOUNDARY_SERVICES:
        try:
            info = c.get(svc, params={"f": "json"}).json()
            fields = [f["name"] for f in info.get("fields", [])]
            code_f = next(f for f in fields if f.upper() in ("UNIT_CODE", "UNITCODE", "PARKCODE", "ALPHA"))
            name_f = next((f for f in fields if f.upper() in ("UNIT_NAME", "UNITNAME", "PARKNAME")), code_f)
            type_f = next((f for f in fields if f.upper() in ("UNIT_TYPE", "UNITTYPE")), None)
            feats, offset = [], 0
            while True:
                q = c.get(f"{svc}/query", params={
                    "where": "1=1", "outFields": ",".join(x for x in (code_f, name_f, type_f) if x),
                    "returnGeometry": "true", "outSR": 4326, "maxAllowableOffset": 0.005,
                    "geometryPrecision": 4, "resultOffset": offset, "resultRecordCount": 200,
                    "f": "geojson"}).json()
                fs = q.get("features", [])
                feats += fs
                if len(fs) < 200 and not q.get("exceededTransferLimit") and not q.get("properties", {}).get("exceededTransferLimit"):
                    break
                offset += len(fs)
                if not fs:
                    break
            keep = []
            for f in feats:
                p = f.get("properties") or {}
                code = str(p.get(code_f) or "").upper()
                if code not in codes or not f.get("geometry"):
                    continue
                keep.append({"type": "Feature", "geometry": f["geometry"],
                             "properties": {"park": code, "name": p.get(name_f),
                                            "unit_type": p.get(type_f) if type_f else None}})
            if not keep:
                raise RuntimeError(f"no features matched from {svc} ({len(feats)} fetched)")
            PARKS.write_text(json.dumps({"type": "FeatureCollection", "features": keep}, separators=(",", ":")))
            report["boundaries"] = {"service": svc, "fields": fields, "fetched": len(feats),
                                    "matched": len({f["properties"]["park"] for f in keep}),
                                    "missing": sorted(codes - {f["properties"]["park"] for f in keep})}
            return
        except Exception as e:
            last = repr(e)
            report.setdefault("boundary_errors", []).append({"service": svc, "error": last})
    raise RuntimeError(last)


def visitation(codes: set[str]) -> None:
    """Annual recreation visits. IRMA's public stats service; several URL shapes tried."""
    rows, tried = [], []
    for code in sorted(codes):
        got = None
        for url in (f"https://irmaservices.nps.gov/v3/rest/stats/visitation?unitCodes={code}&startMonth=1&startYear=2013&endMonth=12&endYear=2020",
                    f"https://irmaservices.nps.gov/v3/rest/stats/total/2013/2020?unitCodes={code}"):
            try:
                r = c.get(url, headers={"Accept": "application/json"})
                if r.status_code != 200:
                    tried.append([url, r.status_code]); continue
                data = r.json()
                recs = data if isinstance(data, list) else data.get("data") or data.get("items") or []
                agg = Counter()
                for rec in recs:
                    y = rec.get("Year") or rec.get("year")
                    v = rec.get("RecreationVisitors") or rec.get("RecreationVisits") or rec.get("recreationVisitors")
                    if y and v is not None:
                        agg[int(y)] += int(float(v))
                if agg:
                    got = agg
                    if not report.get("visitation_sample"):
                        report["visitation_sample"] = {"url": url, "record": recs[0]}
                    break
                tried.append([url, "no usable records", str(data)[:300]])
            except Exception as e:
                tried.append([url, repr(e)])
        if got:
            rows += [{"park": code, "year": y, "visits": v} for y, v in sorted(got.items()) if 2013 <= y <= 2020]
        if not rows and len(tried) >= 8:       # the service shape is wrong; don't hammer it
            break
    report["visitation_tried"] = tried[:12]
    if not rows:
        raise RuntimeError("no visitation data")
    with VISITS.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["park", "year", "visits"]); w.writeheader(); w.writerows(rows)
    report["visitation"] = {"parks": len({r["park"] for r in rows}), "rows": len(rows)}


def mortality_probe() -> None:
    """Columns of the NPS mortality release, for a later section."""
    import openpyxl
    r = c.get(MORTALITY); r.raise_for_status()
    wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
    out = {}
    for ws in wb.worksheets:
        rows = [[("" if v is None else str(v)) for v in row] for row in ws.iter_rows(values_only=True, max_row=8)]
        out[ws.title] = rows
    report["mortality_probe"] = out


def step(name, fn, *a):
    try:
        fn(*a)
    except Exception as e:
        report[f"{name}_error"] = repr(e)
        print(f"  {name}: {e!r} (keeping committed copy)")


def main() -> None:
    step("incidents", incidents)
    codes = set()
    if INC.exists():
        with INC.open() as f:
            codes = {r["park"] for r in csv.DictReader(f) if r["park"]}
    step("boundaries", boundaries, codes)
    step("visitation", visitation, codes)
    step("mortality", mortality_probe)
    REPORT.write_text(json.dumps(report, indent=1, default=str))
    print(json.dumps({k: v for k, v in report.items() if k != "mortality_probe"}, indent=1, default=str)[:4000])


if __name__ == "__main__":
    main()
