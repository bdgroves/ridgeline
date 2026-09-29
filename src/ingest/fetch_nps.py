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
# Incident files use a few non-standard park codes. Sequoia & Kings Canyon report
# together as SEKI but are two units in the boundary and visitation data.
ALIASES = {"AMISTAD": "AMIS", "BISCAYNE": "BISC", "GRANDCANYON": "GRCA"}
COMPOSITE = {"SEKI": ("SEQU", "KICA")}
COMPOSITE_NAMES = {"SEKI": "Sequoia & Kings Canyon National Parks"}

MORTALITY = ("https://www.nps.gov/aboutus/foia/upload/"
             "FOIA-FAQ-NPS-Mortality-Data-CY2007-to-CY2023-Released-August-2023.xlsx")

c = httpx.Client(timeout=180, follow_redirects=True,
                 headers={"User-Agent": "ridgeline (brooksgroves.com)"})
report: dict = {}


def xlsx_rows(content: bytes) -> list[list[str]]:
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    for ws in wb.worksheets:
        rows = [["" if v is None else (v.strftime("%m/%d/%Y") if hasattr(v, "strftime") else str(v).strip())
                 for v in r] for r in ws.iter_rows(values_only=True)]
        for i, r in enumerate(rows):
            if "IncidentNum" in r:
                return [rows[i]] + [x for x in rows[i + 1:] if any(x)]
    return []


def sar_files() -> list[str]:
    """The known SAR lists plus any new one the FOIA page has added since."""
    files = list(FILES)
    try:
        page = c.get("https://www.nps.gov/aboutus/foia/foia-frd.htm").text
        for href in re.findall(r'href="([^"]+\.xlsx)"', page, re.I):
            url = href if href.startswith("http") else "https://www.nps.gov" + href
            name = url.rsplit("/", 1)[-1]
            if re.search(r"sar[-_ ]|search[-_ ]?and[-_ ]?rescue", name, re.I) and url not in files:
                files.append(url)
        report["foia_new_files"] = files[len(FILES):]
    except Exception as e:
        report["foia_page_error"] = repr(e)
    return files


def incidents() -> None:
    seen, out, per_file = set(), [], {}
    for url in sar_files():
        r = c.get(url); r.raise_for_status()
        rows = xlsx_rows(r.content)
        if not rows:
            per_file[url.rsplit("/", 1)[-1]] = {"skipped": "no IncidentNum header"}
            continue
        head, body = rows[0], rows[1:]
        ix = {k: head.index(k) for k in ("IncidentNum", "IncidentDate", "IncType", "ParkAlphaCode", "Region")}
        n = dup = 0
        bad = []
        for row in body:
            iid = row[ix["IncidentNum"]]
            if not iid or iid in seen:
                dup += bool(iid)
                continue
            seen.add(iid)
            raw = row[ix["IncidentDate"]].split(" ")[0]
            if re.match(r"\d{4}-\d{2}-\d{2}$", raw):
                y, m, d = raw.split("-")
            else:
                m, d, y = (raw.split("/") + ["", "", ""])[:3]
            if not (y and m and d):
                bad.append((iid, row[ix["IncidentDate"]], row[ix["ParkAlphaCode"]]))
                continue
            out.append({"id": iid, "date": f"{y}-{int(m):02d}-{int(d):02d}",
                        "type": row[ix["IncType"]],
                        "park": ALIASES.get(row[ix["ParkAlphaCode"]].upper(), row[ix["ParkAlphaCode"]].upper()),
                        "region": row[ix["Region"]]})
            n += 1
        per_file[url.rsplit("/", 1)[-1]] = {"rows": len(body), "kept": n, "duplicate_ids": dup,
                                            "bad_date": len(bad), "bad_date_sample": bad[:5]}
    out.sort(key=lambda r: (r["date"], r["id"]))
    tmp = INC.with_suffix(".tmp")
    with tmp.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "date", "type", "park", "region"])
        w.writeheader(); w.writerows(out)
    tmp.replace(INC)
    report["incidents"] = {"files": per_file, "total": len(out),
                           "first_date": out[0]["date"], "last_date": out[-1]["date"],
                           "no_park_code": sum(1 for r in out if not r["park"])}


def _polys(g: dict) -> list:
    return [g["coordinates"]] if g["type"] == "Polygon" else list(g["coordinates"])


def _ring_area_centroid(ring):
    a = cx = cy = 0.0
    for (x0, y0), (x1, y1) in zip(ring, ring[1:]):
        k = x0 * y1 - x1 * y0
        a += k; cx += (x0 + x1) * k; cy += (y0 + y1) * k
    if abs(a) < 1e-12:
        xs, ys = zip(*ring)
        return 0.0, sum(ys) / len(ys), sum(xs) / len(xs)
    return abs(a) / 2, cy / (3 * a), cx / (3 * a)


def _label_point(polys) -> tuple[float, float]:
    """Centroid of the park's largest polygon, rounded; good enough for a marker."""
    best = max((_ring_area_centroid(p[0]) for p in polys), key=lambda t: t[0])
    return round(best[1], 4), round(best[2], 4)


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
                    "returnGeometry": "true", "outSR": 4326, "maxAllowableOffset": 0.01,
                    "geometryPrecision": 3, "resultOffset": offset, "resultRecordCount": 200,
                    "f": "geojson"}).json()
                fs = q.get("features", [])
                feats += fs
                if len(fs) < 200 and not q.get("exceededTransferLimit") and not q.get("properties", {}).get("exceededTransferLimit"):
                    break
                offset += len(fs)
                if not fs:
                    break
            member_of = {m: k for k, ms in COMPOSITE.items() for m in ms}
            merged: dict[str, dict] = {}
            for f in feats:
                p = f.get("properties") or {}
                code = str(p.get(code_f) or "").upper()
                code = member_of.get(code, code)
                if code not in codes or not f.get("geometry"):
                    continue
                polys = _polys(f["geometry"])
                if code in merged:
                    merged[code]["geometry"]["coordinates"] += polys
                    continue
                merged[code] = {"type": "Feature", "geometry": {"type": "MultiPolygon", "coordinates": polys},
                                "properties": {"park": code, "name": COMPOSITE_NAMES.get(code, p.get(name_f)),
                                               "unit_type": p.get(type_f) if type_f else None}}
            keep = list(merged.values())
            for f in keep:
                f["properties"]["lat"], f["properties"]["lon"] = _label_point(f["geometry"]["coordinates"])
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
        units = ",".join(COMPOSITE.get(code, (code,)))
        for url in (f"https://irmaservices.nps.gov/v3/rest/stats/visitation?unitCodes={units}&startMonth=1&startYear=2013&endMonth=12&endYear=2021",):
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
            rows += [{"park": code, "year": y, "visits": v} for y, v in sorted(got.items()) if 2013 <= y <= 2021]
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
