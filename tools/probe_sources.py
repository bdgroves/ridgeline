"""
Probe candidate open-data sources for rescue calls and record what they
actually contain (fields, samples, call-type vocabularies). Writes
data/external/source_probe.json. Runs in CI because it needs the network.
"""
import csv, io, json, re
from collections import Counter
from pathlib import Path
import httpx

OUT = Path(__file__).resolve().parents[1] / "data" / "external" / "source_probe.json"
RESCUE = re.compile(r"rescue|cliff|hiker|trail|mountain|mtn|fall|lost|search|water|swift|drown|heat|stranded", re.I)
TYPEISH = re.compile(r"type|nature|problem|call|desc|category|incident", re.I)
c = httpx.Client(timeout=60, follow_redirects=True, headers={"User-Agent": "ridgeline-probe (brooksgroves.com)"})
report = {}

def csv_probe(name, url):
    try:
        r = c.get(url); r.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(r.text)))
        fields = list(rows[0].keys()) if rows else []
        out = {"url": url, "rows": len(rows), "fields": fields, "sample": rows[:3]}
        for f in fields:
            if TYPEISH.search(f):
                vc = Counter(r[f] for r in rows)
                out[f"values:{f}"] = {"distinct": len(vc),
                    "rescue_like": {k: v for k, v in vc.most_common() if RESCUE.search(k or "")},
                    "top": vc.most_common(25)}
        report[name] = out
    except Exception as e:
        report[name] = {"url": url, "error": repr(e)}

def hub_probe(name, hub, dsid):
    out = {"hub": hub, "id": dsid}
    try:
        meta = c.get(f"{hub}/api/v3/datasets/{dsid}").json()
        attrs = meta.get("data", {}).get("attributes", {})
        svc = attrs.get("url")
        out.update(service=svc, record_count=attrs.get("recordCount"), name=attrs.get("name"),
                   license=attrs.get("license") or attrs.get("licenseInfo"),
                   modified=attrs.get("modified"))
        info = c.get(svc, params={"f": "json"}).json()
        fields = [(f["name"], f["type"]) for f in info.get("fields", [])]
        out["fields"] = fields
        out["geometryType"] = info.get("geometryType")
        s = c.get(f"{svc}/query", params={"where": "1=1", "outFields": "*", "resultRecordCount": 3,
                                            "f": "json"}).json()
        out["sample"] = [f["attributes"] for f in s.get("features", [])]
        for fname, ftype in fields:
            if ftype == "esriFieldTypeString" and TYPEISH.search(fname):
                st = c.get(f"{svc}/query", params={
                    "where": "1=1", "groupByFieldsForStatistics": fname, "f": "json",
                    "outStatistics": json.dumps([{"statisticType": "count", "onStatisticField": fname,
                                                  "outStatisticFieldName": "n"}])}).json()
                vals = Counter({str(f["attributes"].get(fname)): f["attributes"]["n"] for f in st.get("features", [])})
                out[f"values:{fname}"] = {"distinct": len(vals),
                    "rescue_like": {k: v for k, v in vals.most_common() if RESCUE.search(k)},
                    "top": vals.most_common(25)}
        # date span
        for fname, ftype in fields:
            if ftype == "esriFieldTypeDate":
                st = c.get(f"{svc}/query", params={"where": "1=1", "f": "json", "outStatistics": json.dumps([
                    {"statisticType": "min", "onStatisticField": fname, "outStatisticFieldName": "lo"},
                    {"statisticType": "max", "onStatisticField": fname, "outStatisticFieldName": "hi"}])}).json()
                out[f"span:{fname}"] = st.get("features", [{}])[0].get("attributes")
    except Exception as e:
        out["error"] = repr(e)
    report[name] = out

csv_probe("san_diego_2025", "https://seshat.datasd.org/fire_ems_incidents/fd_incidents_2025_datasd.csv")
hub_probe("scottsdale_fire_cfs", "https://data.scottsdaleaz.gov", "187dd861ed1c4aecb392fce2ee901c05_20")
hub_probe("boulder_fire_unit_response", "https://open-data.bouldercolorado.gov", "6ae16fc5d05b46189800f189a587a223_0")
hub_probe("boulder_response_times", "https://open-data.bouldercolorado.gov", "18c58aed5261498980e61b1e58eed376_0")

OUT.write_text(json.dumps(report, indent=1, default=str))
print(json.dumps({k: {kk: (vv if not isinstance(vv, (list, dict)) else "…") for kk, vv in v.items()} for k, v in report.items()}, indent=1))
