"""
Probe three sources needed next and record what they contain. Runs in CI.

1. NPS FOIA search-and-rescue incident spreadsheets (nps.gov/aboutus/foia/foia-frd.htm):
   columns, row counts, rows per park, Yosemite samples.
2. NWS heat warnings for the Phoenix zones from the Iowa Environmental Mesonet
   VTEC archive (EH = Excessive Heat, renamed XH = Extreme Heat in 2025).
3. Phoenix's 2024 trails-and-heat-safety review PDF (text, for closure data).

Writes data/external/probe_heat_nps.json.
"""
import io, json, re
from collections import Counter
from pathlib import Path
import httpx

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "external" / "probe_heat_nps.json"
c = httpx.Client(timeout=120, follow_redirects=True,
                 headers={"User-Agent": "ridgeline-probe (brooksgroves.com)"})
report = {}


def safe(name, fn):
    try:
        report[name] = fn()
    except Exception as e:
        report[name] = {"error": repr(e)}


# ── 1. NPS FOIA SAR spreadsheets ────────────────────────────────────────────
def nps():
    import openpyxl
    page = "https://www.nps.gov/aboutus/foia/foia-frd.htm"
    html = c.get(page).text
    links = sorted(set(re.findall(r'href="([^"]+\.(?:xlsx|xls|csv))"', html, re.I)))
    links = [l if l.startswith("http") else "https://www.nps.gov" + l for l in links]
    out = {"page": page, "all_links": links, "files": {}}
    sar = [l for l in links if re.search(r"sar|search|rescue", l, re.I)] or links
    for url in sar[:6]:
        f = {"url": url}
        try:
            r = c.get(url); r.raise_for_status()
            f["bytes"] = len(r.content)
            wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
            f["sheets"] = {}
            for ws in wb.worksheets:
                rows = [[("" if v is None else str(v)) for v in row] for row in ws.iter_rows(values_only=True)]
                # header = first row with >= 3 non-empty cells
                hi = next((i for i, row in enumerate(rows) if sum(bool(x.strip()) for x in row) >= 3), 0)
                head = rows[hi] if rows else []
                body = [row for row in rows[hi + 1:] if any(x.strip() for x in row)]
                s = {"header_row": hi, "columns": head, "rows": len(body), "sample": body[:3]}
                for j, col in enumerate(head):
                    if re.search(r"park|unit|alpha|code", col, re.I):
                        vc = Counter(row[j] for row in body if j < len(row))
                        s[f"values:{col}"] = {"distinct": len(vc), "top": vc.most_common(30)}
                yo = [row for row in body if any(re.search(r"yose|yosemite", x, re.I) for x in row)]
                s["yosemite_rows"] = len(yo)
                s["yosemite_sample"] = yo[:5]
                for j, col in enumerate(head):
                    vc = Counter(row[j] for row in body if j < len(row))
                    if 1 < len(vc) <= 60:
                        s[f"vocab:{col}"] = vc.most_common(60)
                f["sheets"][ws.title] = s
        except Exception as e:
            f["error"] = repr(e)
        out["files"][url.rsplit("/", 1)[-1]] = f
    return out


# ── 2. NWS heat warnings, Phoenix ───────────────────────────────────────────
def iem():
    out = {}
    try:
        ugcs = c.get("https://mesonet.agron.iastate.edu/api/1/nws/ugcs.json",
                     params={"state": "AZ"}).json()
        rows = ugcs.get("data", ugcs)
        out["az_zones_phoenixish"] = [u for u in rows
            if re.search(r"phoenix|valley|scottsdale|paradise|camelback|maricopa|tempe|mesa|glendale",
                         json.dumps(u), re.I)][:60]
    except Exception as e:
        out["ugcs_error"] = repr(e)
    for ugc in ["AZZ023", "AZZ537", "AZZ540", "AZZ541", "AZZ542", "AZZ543", "AZZ544",
                "AZZ545", "AZZ546", "AZZ547", "AZZ548", "AZZ549", "AZZ550", "AZZ551"]:
        try:
            r = c.get("https://mesonet.agron.iastate.edu/json/vtec_events_byugc.php",
                      params={"ugc": ugc, "sdate": "2018-01-01", "edate": "2026-09-30"})
            j = r.json()
            ev = j.get("events", j if isinstance(j, list) else [])
            heat = [e for e in ev if str(e.get("phenomena")) in ("EH", "XH") and str(e.get("significance")) == "W"]
            out[ugc] = {"n_events": len(ev), "n_heat_warnings": len(heat),
                        "keys": list(ev[0].keys()) if ev else [], "heat_sample": heat[:3],
                        "name": (ev[0].get("name") if ev else None)}
        except Exception as e:
            out[ugc] = {"error": repr(e)}
    return out


# ── 3. Phoenix heat-safety review PDF ───────────────────────────────────────
def pdf():
    from pypdf import PdfReader
    url = ("https://www.phoenix.gov/content/dam/phoenix/parkssite/documents/"
           "2024-10-24%20phoenix%20trails%20and%20heat%20safety.pdf")
    r = c.get(url); r.raise_for_status()
    rd = PdfReader(io.BytesIO(r.content))
    pages = [(p.extract_text() or "") for p in rd.pages]
    txt = "\n\n---page---\n\n".join(pages)
    return {"url": url, "pages": len(pages), "text": txt[:60000]}


# ── 4. Daily weather, Phoenix Sky Harbor, for the heat analysis ─────────────
def weather():
    r = c.get("https://archive-api.open-meteo.com/v1/archive", params={
        "latitude": 33.4342, "longitude": -112.0116, "start_date": "2018-01-01",
        "end_date": "2026-09-20", "timezone": "America/Phoenix", "temperature_unit": "fahrenheit",
        "daily": "temperature_2m_max,temperature_2m_min,apparent_temperature_max,precipitation_sum"})
    r.raise_for_status()
    d = r.json()["daily"]
    keys = list(d.keys())
    lines = [",".join(keys)] + [",".join("" if d[k][i] is None else str(d[k][i]) for k in keys)
                                for i in range(len(d["time"]))]
    (ROOT / "data" / "external" / "phoenix_daily_weather.csv").write_text("\n".join(lines) + "\n")
    return {"days": len(d["time"]), "columns": keys}


safe("weather", weather)
safe("nps_foia", nps)
safe("iem_heat", iem)
safe("phoenix_heat_pdf", pdf)
OUT.write_text(json.dumps(report, indent=1, default=str))
print(json.dumps({k: (list(v.keys()) if isinstance(v, dict) else v) for k, v in report.items()}, indent=1))
