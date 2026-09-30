"""
ridgeline / src / viz / build_site.py

Builds the Ridgeline site (site/index.html) from the committed city files:

    data/external/phoenix_sar_incidents.geojson     + geocode_report.json
    data/external/scottsdale_sar_incidents.geojson  + scottsdale_report.json
    data/external/boulder_sar_incidents.geojson     + boulder_report.json

Everything on the page is computed here from those files, so the page can be
rebuilt anywhere without the raw downloads. Charts are inline SVG; the map is
Leaflet reading the same GeoJSON, copied to site/data/.

Run:
    pixi run build
"""

from __future__ import annotations

import html
import json
import shutil
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

from nps_section import NPS_CSS, nps_data, nps_html, nps_js

ROOT     = Path(__file__).resolve().parents[2]
EXT_DIR  = ROOT / "data" / "external"
SITE_DIR = ROOT / "site"

MONTHS   = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"]
MONTHS_L = ["January", "February", "March", "April", "May", "June", "July",
            "August", "September", "October", "November", "December"]
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

COLORS = {"mountain": "#e8793a", "water": "#5aa3c0", "search": "#c8a96e"}
CAT_LABEL = {"mountain": "Mountain & technical rescue",
             "water": "Flood & water rescue",
             "search": "Land search"}

# Dispatch addresses that are well-known trailheads. Only places whose
# address is unambiguous are named; everything else shows as logged.
TRAILHEADS = {
    "49XX E MCDONALD DR":                "Echo Canyon Trailhead · Camelback",
    "4925 East MCDONALD Drive":          "Echo Canyon Trailhead · Camelback",
    "57XX N ECHO CANYON PW":             "Echo Canyon Pkwy · Camelback",
    "27XX E PIESTEWA PEAK DR":           "Piestewa Peak · park road",
    "27XX E SQUAW PEAK DR":              "Piestewa Peak · park road (old name)",
    "47XX E PIMA CANYON RD":             "Pima Canyon Trailhead · South Mountain",
    "62XX E CHOLLA LN":                  "Cholla Trailhead · Camelback",
    "51XX N INVERGORDON RD":             "Cholla Trailhead · Camelback (address since 2022)",
    "7XX E DESERT FOOTHILLS PW":         "Desert Foothills Pkwy · South Mountain",
    "24XX E VALLEY VIEW DR":             "Valley View Dr · South Mountain, north side",
    "109XX S CENTRAL AV":                "South Mountain Park entrance",
    "18333 North THOMPSON PEAK Parkway": "Gateway Trailhead · McDowell Sonoran",
    "23015 North 128TH Street":          "Tom's Thumb Trailhead · McDowell Sonoran",
    "30301 North ALMA SCHOOL Parkway":   "Brown's Ranch Trailhead · McDowell Sonoran",
}

# Boulder publishes points, not addresses. Points within ~100 m of these
# spots are named; the rest are listed by coordinate.
BOULDER_PLACES = [
    ((39.9993, -105.2826), "Chautauqua Park · Flatirons trailheads"),
    ((39.9782, -105.2754), "NCAR Mesa trailhead"),
    ((39.9591, -105.3002), "One shared point in the mountain parks near Bear Peak"),
]


def boulder_place(lat: float, lon: float) -> str:
    for (py, px), name in BOULDER_PLACES:
        if abs(lat - py) < 0.0012 and abs(lon - px) < 0.0016:
            return name
    return f"{lat:.3f}, {lon:.3f}"


CITIES = [
    {
        "key": "phoenix", "name": "Phoenix", "state": "AZ",
        "agency": "Phoenix Fire Department",
        "source_name": "Phoenix Open Data · Fire calls for service",
        "source_url": "https://www.phoenixopendata.com/dataset/caf49f72-f22f-4ad9-9405-2a3db9619423",
        "license": "CC BY",
        "center": [33.52, -112.03], "zoom": 11,
        "location_note": "Addresses are published to the hundred block and geocoded here.",
        "years_full": (2019, 2025),
    },
    {
        "key": "scottsdale", "name": "Scottsdale", "state": "AZ",
        "agency": "Scottsdale Fire Department",
        "source_name": "Scottsdale Open Data · Fire Department calls for service",
        "source_url": "https://data.scottsdaleaz.gov/datasets/187dd861ed1c4aecb392fce2ee901c05_20",
        "license": "City of Scottsdale open data terms",
        "center": [33.64, -111.85], "zoom": 11,
        "location_note": "Full street addresses, geocoded here. Dates only, no time of day.",
        "years_full": (2023, 2025),
    },
    {
        "key": "boulder", "name": "Boulder", "state": "CO",
        "agency": "Boulder Fire-Rescue",
        "source_name": "Boulder Open Data · Response times for Boulder Fire-Rescue",
        "source_url": "https://open-data.bouldercolorado.gov/datasets/18c58aed5261498980e61b1e58eed376_0",
        "license": "CC0 1.0",
        "center": [40.00, -105.29], "zoom": 12,
        "location_note": "Points published by the city; no street addresses.",
        "years_full": (2015, 2025),
    },
]


# ── data ─────────────────────────────────────────────────────────────────────

def phoenix_category(t: str) -> str | None:
    t = (t or "").lower()
    if any(k in t for k in ("mountain rescue", "tree rescue", "confined space", "rescue service")):
        return "mountain"
    if any(k in t for k in ("water rescue", "swift water", "flooding", "stranded")):
        return "water"
    return None


def load_city(city: dict) -> list[dict]:
    path = EXT_DIR / f"{city['key']}_sar_incidents.geojson"
    if not path.exists():
        return []
    rows = []
    for f in json.loads(path.read_text())["features"]:
        p = dict(f["properties"])
        if city["key"] == "phoenix":
            p["category"] = phoenix_category(p.get("incident_type"))
        if not p.get("category") or not p.get("date"):
            continue
        d = date.fromisoformat(str(p["date"])[:10])
        p.update(year=d.year, month=d.month, weekday=d.weekday())
        p["lon"], p["lat"] = f["geometry"]["coordinates"]
        if city["key"] == "boulder":
            p["place"] = boulder_place(p["lat"], p["lon"])
        rows.append(p)
    return rows


def load_json(name: str) -> dict:
    p = EXT_DIR / name
    return json.loads(p.read_text()) if p.exists() else {}


def city_stats(city: dict, rows: list[dict]) -> dict:
    m = [r for r in rows if r["category"] == "mountain"]
    by_cat = Counter(r["category"] for r in rows)
    years = sorted({r["year"] for r in rows})
    y0, y1 = city["years_full"]
    annual = Counter(r["year"] for r in m)
    hours = Counter(r["hour"] for r in m if r.get("hour") not in (None, ""))
    locs = Counter(r.get("place") or r["location_name"] for r in m if r.get("place") or r.get("location_name"))
    prec = Counter(r.get("precision") or "unknown" for r in rows)
    full = [annual.get(y, 0) for y in range(y0, y1 + 1)]
    months = Counter(r["month"] for r in m)
    return {
        "rows": rows, "mountain": m, "by_cat": by_cat,
        "years": (years[0], years[-1]) if years else (None, None),
        "annual": annual, "months": months,
        "weekdays": Counter(r["weekday"] for r in m),
        "hours": hours, "top_locations": locs.most_common(8),
        "precision": prec,
        "per_year": round(sum(full) / len(full)) if full else 0,
        "weekend_pct": round(100 * sum(r["weekday"] >= 5 for r in m) / len(m), 1) if m else 0,
        "busiest_month": MONTHS_L[months.most_common(1)[0][0] - 1] if months else "—",
        "quietest_month": MONTHS_L[min(range(1, 13), key=lambda k: months.get(k, 0)) - 1] if months else "—",
    }


def city_series(city: dict, rows: list[dict]) -> dict:
    """Per-category counts the page redraws from when the map buttons change."""
    y0 = min(r["year"] for r in rows)
    y1 = max(r["year"] for r in rows)
    fy0, fy1 = city["years_full"]
    out = {"years": list(range(y0, y1 + 1)), "full": [fy0, fy1],
           "has_hours": any(r.get("hour") not in (None, "") for r in rows), "cats": {}}
    for cat in COLORS:
        rs = [r for r in rows if r["category"] == cat]
        if not rs:
            continue
        ann, mon = Counter(r["year"] for r in rs), Counter(r["month"] for r in rs)
        hrs, wd = Counter(r["hour"] for r in rs if r.get("hour") not in (None, "")), Counter(r["weekday"] for r in rs)
        locs = Counter(r.get("place") or r["location_name"] for r in rs if r.get("place") or r.get("location_name"))
        out["cats"][cat] = {
            "total":   len(rs),
            "annual":  [ann.get(y, 0) for y in out["years"]],
            "months":  [mon.get(m, 0) for m in range(1, 13)],
            "hours":   [hrs.get(h, 0) for h in range(24)],
            "weekdays": [wd.get(d, 0) for d in range(7)],
            "weekend": sum(r["weekday"] >= 5 for r in rs),
            "top":     [[TRAILHEADS.get(a, a), a if a in TRAILHEADS else "", n] for a, n in locs.most_common(15)],
        }
    return out


# ── svg charts ───────────────────────────────────────────────────────────────

def bar_chart(values: list[int], labels: list[str], color: str, *, height: int = 150,
              label_every: int = 1, faded: set[int] | None = None, title: str = "",
              show_values: bool = True) -> str:
    """Vertical bars as inline SVG. `faded` indexes render lighter (partial years)."""
    faded = faded or set()
    n = len(values)
    w, pad_b, pad_t = 600, 22, 18
    gap = 4 if n < 16 else 2
    bw = (w - gap * (n - 1)) / n
    vmax = max(values) if values and max(values) > 0 else 1
    h = height - pad_b - pad_t
    parts = [f'<svg viewBox="0 0 {w} {height}" role="img" aria-label="{html.escape(title)}" preserveAspectRatio="none">']
    parts.append(f'<line x1="0" y1="{pad_t + h}" x2="{w}" y2="{pad_t + h}" class="axis"/>')
    for i, v in enumerate(values):
        bh = h * v / vmax
        x = i * (bw + gap)
        y = pad_t + h - bh
        op = "0.35" if i in faded else "1"
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{bh:.1f}" rx="1.5" '
                     f'fill="{color}" opacity="{op}"><title>{html.escape(labels[i])}: {v:,}</title></rect>')
        if show_values and n <= 14 and v:
            parts.append(f'<text x="{x + bw / 2:.1f}" y="{y - 5:.1f}" class="val">{v:,}</text>')
        if i % label_every == 0:
            parts.append(f'<text x="{x + bw / 2:.1f}" y="{height - 6}" class="lab">{html.escape(labels[i])}</text>')
    parts.append("</svg>")
    return "".join(parts)


# ── html pieces ──────────────────────────────────────────────────────────────

def esc(s) -> str:
    return html.escape(str(s))


def kpi(value: str, label: str, sub: str = "") -> str:
    return (f'<div class="kpi"><div class="kpi-v">{value}</div>'
            f'<div class="kpi-l">{esc(label)}</div>'
            + (f'<div class="kpi-s">{sub}</div>' if sub else "") + "</div>")


def city_panel(city: dict, s: dict, report: dict) -> str:
    k = city["key"]
    y0, y1 = s["years"]
    years = list(range(y0, y1 + 1)) if y0 else []
    fy0, fy1 = city["years_full"]
    faded = {i for i, y in enumerate(years) if y < fy0 or y > fy1}
    annual = bar_chart([s["annual"].get(y, 0) for y in years], [str(y) for y in years],
                       COLORS["mountain"], faded=faded, title=f"{city['name']} mountain rescues per year",
                       label_every=1 if len(years) <= 12 else 2)
    months = bar_chart([s["months"].get(m, 0) for m in range(1, 13)], MONTHS, COLORS["mountain"],
                       title=f"{city['name']} mountain rescues by month")
    weekdays = bar_chart([s["weekdays"].get(d, 0) for d in range(7)], WEEKDAYS, COLORS["mountain"],
                         title=f"{city['name']} mountain rescues by weekday")
    if s["hours"]:
        hours_svg = bar_chart([s["hours"].get(h, 0) for h in range(24)],
                              [f"{h:02d}" for h in range(24)], COLORS["mountain"], label_every=3,
                              show_values=False, title=f"{city['name']} mountain rescues by hour")
        hours_block = f'<figure class="chart"><figcaption>By hour of the call</figcaption>{hours_svg}</figure>'
    else:
        hours_block = ('<figure class="chart muted-box"><figcaption>By hour of the call</figcaption>'
                       '<p>Scottsdale publishes the date of each call but not the time.</p></figure>')

    if s["top_locations"]:
        top_n = s["top_locations"][0][1]
        rows = "".join(
            f'<tr><td><span class="loc">{esc(TRAILHEADS.get(a, a))}</span>'
            + (f'<span class="addr">{esc(a)}</span>' if a in TRAILHEADS else "")
            + f'</td><td class="num">{n:,}</td><td class="barcell"><span style="width:{100 * n / top_n:.0f}%"></span></td></tr>'
            for a, n in s["top_locations"])
        places = (f'<table class="places"><thead><tr><th>Dispatch location</th><th class="num">Calls</th><th></th></tr></thead>'
                  f'<tbody>{rows}</tbody></table>')
    else:
        places = '<p class="muted">No locations recorded.</p>'
    if k == "boulder":
        places += ('<p class="source">Boulder publishes a point for each call rather than an address, so places are '
                   'named from the coordinates. Many calls share a single point, which likely stands in for a '
                   'trail or open-space area rather than the exact spot.</p>')

    located = report.get("located"), report.get("incidents")
    if k == "phoenix":
        mr = report.get("mountain_rescue", {})
        loc_line = (f'{mr.get("located", 0):,} of {mr.get("rows", 0):,} mountain rescues located '
                    f'({100 * mr.get("located", 0) / max(mr.get("rows", 1), 1):.0f}%)')
    elif located[1]:
        loc_line = f"{located[0]:,} of {located[1]:,} calls located ({100 * located[0] / located[1]:.0f}%)"
    else:
        loc_line = ""

    return f"""
<section class="city" id="city-{k}" data-city="{k}" {'hidden' if k != 'phoenix' else ''}>
  <div class="kpis" data-part="kpis">
    {kpi(f'{len(s["mountain"]):,}', "Mountain & technical rescues mapped", f'{y0}–{y1}')}
    {kpi(f'~{s["per_year"]:,}', "Per full year", f'{fy0}–{fy1} average')}
    {kpi(esc(s["busiest_month"]), "Busiest month", f'quietest: {esc(s["quietest_month"])}')}
    {kpi(f'{s["weekend_pct"]}%', "On weekends", "2 of 7 days = 29%")}
  </div>
  <div class="grid2">
    <figure class="chart" data-part="annual"><figcaption>Mountain &amp; technical rescues per year{' · faded = partial year' if faded else ''}</figcaption>{annual}</figure>
    <figure class="chart" data-part="months"><figcaption>By month, all years</figcaption>{months}</figure>
    {hours_block.replace('<figure class="chart"', '<figure class="chart" data-part="hours"', 1)}
    <figure class="chart" data-part="weekdays"><figcaption>By day of week</figcaption>{weekdays}</figure>
  </div>
  <h3>Where the calls come from</h3>
  <div data-part="places">{places}</div>
  <p class="source">Source: <a href="{esc(city['source_url'])}">{esc(city['source_name'])}</a> · {esc(city['license'])} ·
     {esc(city['location_note'])} {esc(loc_line)}.</p>
</section>"""


def heat_section(h: dict) -> str:
    """Phoenix heat warnings, daily highs and the trail-closure program."""
    if not h or "warning_vs_not" not in h:
        return ""
    ww = {(r["period"], r["days"]): r for r in h["warning_vs_not"]}
    pre_w, pre_o = ww[("before", "warning days")], ww[("before", "other May–Sep days")]
    post_w, post_o = ww[("after", "warning days")], ww[("after", "other May–Sep days")]
    cy = {int(k): v for k, v in h.get("closure_days_by_year", {}).items()}
    pol = h.get("policy", [])
    def _d(iso):
        from datetime import date as _date
        x = _date.fromisoformat(iso); return f"{x.strftime('%b')} {x.day}, {x.year}"
    policy_rows = "".join(
        f'<tr><td class="num">{_d(p["from"])}</td><td class="num">{esc(p["hours"].replace(":00", "").replace("-", "–"))}</td>'
        f'<td>{esc("Watch or warning" if p["trigger"] == "watch or warning" else "Warning")}</td><td>{esc(p["note"])}</td></tr>'
        for p in pol)
    did = h.get("diff_in_diff", {})
    wy = {int(k): v for k, v in h["warning_days_by_year"].items()}
    span_years = [y for y in range(2019, int(h["span"][1][:4]) + 1)]
    post_years = [y for y in span_years if y >= 2021]
    avg_warn = round(sum(wy.get(y, 0) for y in post_years) / max(len(post_years), 1))
    ab = {int(k): v for k, v in h["all_by_year"].items()}
    c19 = ab.get(2019, {}).get("closure", 0)
    last3 = [y for y in span_years][-3:]
    c_late = round(sum(ab.get(y, {}).get("closure", 0) for y in last3) / 3)
    o19 = ab.get(2019, {}).get("other", 0) + ab.get(2019, {}).get("south", 0)
    o_late = round(sum(ab.get(y, {}).get("other", 0) + ab.get(y, {}).get("south", 0) for y in last3) / 3)
    # Upper bound on rescues the closures could have prevented per year: the whole
    # warning-day drop at the closure trails, times warning days per year.
    saved = (pre_w["closure_per_100_days"] - post_w["closure_per_100_days"]) / 100 * avg_warn
    n_closed = len(h.get("closed_hour_calls", []))

    def row(label, kind):
        cells = "".join(f'<td class="num">{r[kind + "_per_100_days"]:.0f}</td>' for r in (pre_w, pre_o, post_w, post_o))
        d = did.get(kind)
        eff = (f'<td class="num">{d["ratio"]:.2f}<br><small>{d["ci95"][0]:.2f}–{d["ci95"][1]:.2f}</small></td>'
               if d else '<td class="num">–</td>')
        return f"<tr><td>{label}</td>{cells}{eff}</tr>"

    temp = h["by_temperature"]
    t_labels = [t["bin"] for t in temp]
    t_closure = bar_chart([t["closure_per_100_days"] or 0 for t in temp], t_labels, COLORS["mountain"],
                          title="Rescues per 100 days at the closure trails, by daily high")
    t_other = bar_chart([t["other_per_100_days"] or 0 for t in temp], t_labels, "#8a8676",
                        title="Rescues per 100 days on other Phoenix trails, by daily high")
    warn_svg = bar_chart([wy.get(y, 0) for y in span_years], [str(y) for y in span_years], "#d9534f",
                         title="NWS heat-warning days per year, Central Phoenix")
    yr_closure = bar_chart([ab.get(y, {}).get("closure", 0) for y in span_years], [str(y) for y in span_years],
                           COLORS["mountain"], title="Rescues per year at the closure trails")
    yr_other = bar_chart([ab.get(y, {}).get("other", 0) + ab.get(y, {}).get("south", 0) for y in span_years],
                         [str(y) for y in span_years], "#8a8676", title="Rescues per year on other Phoenix trails")
    hrs = h["closure_hours_on_warning_days"]["after"]
    hr_svg = bar_chart(hrs, [f"{i:02d}" for i in range(24)], COLORS["mountain"], label_every=3, show_values=False,
                       title="Closure-trail rescues on warning days since 2021, by hour")
    closed_list = "".join(
        f'<tr><td class="num">{esc(c["date"])}</td><td class="num">{c["hour"]:02d}:00</td>'
        f'<td>{esc(TRAILHEADS.get(c["address"], c["address"]))}</td><td class="num">{esc(c.get("window", ""))}</td><td class="num">{c["high"]:.0f}°F</td></tr>'
        for c in h.get("closed_hour_calls", []))
    cd = did.get("closure", {})
    cf = h.get("city_figures")
    city_block = ""
    if cf:
        yrs = [y for y in (2021, 2022, 2023, 2024)]
        pub = {int(k): v for k, v in cf["published"].items()}
        mo = {int(k): v for k, v in cf["ours_may_oct"].items()}
        na = {int(k): v for k, v in cf["ours_nov_apr"].items()}
        ch = {int(k): v for k, v in cf["ours_closed_hours_on_warning_days"].items()}
        def pct(d):
            return f"{100 * (d.get(yrs[-1], 0) / max(d.get(yrs[0], 1), 1) - 1):+.0f}%"
        cells = lambda d: "".join(f'<td class="num">{d.get(y, 0)}</td>' for y in yrs)
        city_block = f"""
  <h3>The city's numbers, reproduced</h3>
  <div class="scroll"><table>
    <thead><tr><th>Closure trails (Echo Canyon, Cholla, Piestewa)</th>{"".join(f'<th class="num">{y}</th>' for y in yrs)}<th class="num">{yrs[0]}&rarr;{yrs[-1]}</th></tr></thead>
    <tbody>
      <tr><td>City of Phoenix, "rescues on closed trails"</td>{cells(pub)}<td class="num">{pct(pub)}</td></tr>
      <tr><td>Dispatch data, May–October, any day or hour</td>{cells(mo)}<td class="num">{pct(mo)}</td></tr>
      <tr><td>Dispatch data, November–April (no closures)</td>{cells(na)}<td class="num">{pct(na)}</td></tr>
      <tr><td>Dispatch data, during closed hours on warning days</td>{cells(ch)}<td class="num"></td></tr>
    </tbody>
  </table></div>
  <p class="source">The city published these four figures in its
  <a href="{esc(cf["source"])}">October 2024 news release</a> without a definition. Counting every mountain-rescue call at
  the closure trails from May through October, on any day and at any hour, reproduces them to within 3 a year, so
  "on closed trails" most likely means <i>trails that close</i>, not rescues while they were closed. The same trails fell
  just as much over the winter months, when nothing closes.</p>
"""
    return f"""
<section class="block" id="heat"><div class="wrap">
  <h2>Heat &amp; trail closures</h2>
  <div class="prose">
  <p>Since July 2021 Phoenix has closed the <b>Echo Canyon and Cholla trails on Camelback</b> and the <b>Piestewa Peak
  trails</b> on National Weather Service heat days, and added South Mountain in October 2024. The hours, season and
  trigger changed several times (table below), so every date here is scored under the rules in force that day. Does the
  dispatch data show the closures working? This section lines up all {h["calls"]:,} Phoenix mountain-rescue calls, located
  or not, with daily highs at Sky Harbor and every heat warning and watch the Weather Service issued for Central Phoenix.</p>
  </div>
  <div class="scroll"><table>
    <thead><tr><th class="num">From</th><th>Closed</th><th>Trigger</th><th>Rules</th></tr></thead>
    <tbody>{policy_rows}</tbody>
  </table></div>
  <p class="source">Timeline shared by Yun-Peng (Liz) Lu, University of Maryland, and checked against
  <a href="https://cronkitenews.azpbs.org/2021/07/15/hiking-trails-on-piestewa-camelback-will-close-when-temperatures-hit-105/">2021</a> and
  <a href="https://cronkitenews.azpbs.org/2023/09/08/phoenix-hiking-trails-camelback-mountain-piestewa-peak-heat-warning-closures/">2023</a>
  coverage and the city's <a href="https://www.phoenix.gov/newsroom/parks-news/3256.html">2024 release</a>.</p>
  <div class="kpis">
    {kpi(f"{avg_warn}", "Warning days a year", f"{post_years[0]}–{post_years[-1]} average")}
    {kpi(f'{pre_w["closure_per_100_days"]:.0f} &rarr; {post_w["closure_per_100_days"]:.0f}', "Closure-trail rescues", "per 100 heat days, before &rarr; closure days after")}
    {kpi(f'{pre_o["closure_per_100_days"]:.0f} &rarr; {post_o["closure_per_100_days"]:.0f}', "Same trails, other days", "per 100 ordinary May–Sep days")}
    {kpi(f"{n_closed}", "Rescues during closed hours", "closed trails, closure days, since July 2021")}
  </div>

  <h3>Warning days vs. ordinary summer days</h3>
  <div class="scroll"><table>
    <thead><tr><th>Rescues per 100 days, May–Sep</th><th class="num">Before<br>heat days</th><th class="num">Before<br>ordinary</th>
      <th class="num">After<br>closure days</th><th class="num">After<br>ordinary</th><th class="num">Net change<br><small>95% range</small></th></tr></thead>
    <tbody>
      {row("Closure trails (Camelback, Piestewa)", "closure")}
      {row("South Mountain (closed from Oct 2024)", "south")}
      {row("All other Phoenix trails", "other")}
    </tbody>
  </table></div>
  <p class="source">Before = January 2019 to July 15, 2021; after = from the pilot's start on July 16, 2021. Before,
  a heat day is one with a warning in effect between 11&nbsp;a.m. and 5&nbsp;p.m. (the first rule); after, a closure day is one
  with the trigger in effect during that day's closed hours. Days: {pre_w["n_days"]} heat and {pre_o["n_days"]} ordinary
  May–Sep days before, {post_w["n_days"]} and {post_o["n_days"]} after. "Net change" compares the heat-day rate with the same
  trails' ordinary-day rate, after vs before; 1.00 means no change, below 1 means heat days got relatively quieter. The
  range is an approximate 95% interval from the call counts.</p>

  <div class="grid2" style="margin-top:18px">
    <figure class="chart"><figcaption>Closure trails · rescues per 100 days, by daily high (°F)</figcaption>{t_closure}</figure>
    <figure class="chart"><figcaption>Other Phoenix trails · rescues per 100 days, by daily high (°F)</figcaption>{t_other}</figure>
    <figure class="chart"><figcaption>Closure trails · rescues per year, all months</figcaption>{yr_closure}</figure>
    <figure class="chart"><figcaption>Other Phoenix trails (incl. South Mountain) · rescues per year</figcaption>{yr_other}</figure>
    <figure class="chart"><figcaption>NWS heat-warning days per year · Central Phoenix</figcaption>{warn_svg}</figure>
    <figure class="chart"><figcaption>Closure trails · rescues on closure days since July 2021, by hour</figcaption>{hr_svg}</figure>
  </div>

  <h3>What it says</h3>
  <div class="defs">
    <div class="def"><h4>Heat days got quieter at the closed trails</h4><p>Before the program the closure trails
      averaged {pre_w["closure_per_100_days"]:.0f} rescues per 100 heat days; on closure days since, {post_w["closure_per_100_days"]:.0f}. Other trails
      barely moved ({pre_w["other_per_100_days"]:.0f} &rarr; {post_w["other_per_100_days"]:.0f}). That's the pattern you'd expect if closures
      work. The net change is {cd.get("ratio", 0):.2f}, about a {round(100 * (1 - cd.get("ratio", 1)))}% relative drop, with a 95% range of
      {cd.get("ci95", [0, 0])[0]:.2f} to {cd.get("ci95", [0, 0])[1]:.2f}{", which only just excludes no effect" if cd.get("ci95", [0, 1])[1] < 1 else ", which includes no effect at all"}.
      The counts are small ({cd.get("counts", [0]*4)[0]} and {cd.get("counts", [0]*4)[2]} calls), so treat it as a signal, not a measurement.</p></div>
    <div class="def"><h4>Closures can't explain the long drop</h4><p>The closure trails went from {c19} rescues in 2019 to about
      {c_late} a year in {last3[0]}–{last3[-1]}, while other trails went from {o19} to about {o_late}. But the drop is year-round,
      and there are only about {avg_warn} warning days a year. Even if every warning-day rescue that disappeared was prevented by
      a closure, that's about {saved:.0f} a year. Something else, most of it outside summer, did the rest.</p></div>
    <div class="def"><h4>Rescues don't rise with the thermometer</h4><p>Calls per day are roughly flat from below 70°F
      to 110°F and up. At the closure trails, 110-degree days are as quiet as the coolest ones. The city's own review found visitors fall
      as the temperature climbs, so each hiker out there faces more risk even though the count of rescues doesn't grow.</p></div>
    <div class="def"><h4>Some people go anyway</h4><p>Since the pilot began there have been {n_closed} rescues at closed
      trails during that day's closed hours, none in 2021–22 and most since the 2023 switch to 9&nbsp;a.m. Some may have started
      before the closure; some may be at the trailhead rather than on the trail. The list is below.</p></div>
  </div>

{city_block}
  <details class="more"><summary>Rescues at closure trails during closed hours ({n_closed})</summary>
    <div class="scroll"><table><thead><tr><th class="num">Date</th><th class="num">Hour</th><th>Where</th><th class="num">Closed</th><th class="num">High</th></tr></thead>
    <tbody>{closed_list}</tbody></table></div>
  </details>
  <p class="source">Heat warnings and watches for zone {esc(h["zone"])}, Central Phoenix, come from the
  <a href="https://mesonet.agron.iastate.edu/vtec/">Iowa Environmental Mesonet VTEC archive</a>. Counting every date a
  warning touched gives {", ".join(str(wy.get(y, 0)) for y in (2021, 2022, 2023, 2024))} days for 2021–2024, the same as the city's
  <a href="https://www.phoenix.gov/content/dam/phoenix/parkssite/documents/2024-10-24%20phoenix%20trails%20and%20heat%20safety.pdf">2024 program review</a>
  (20, 18, 42, 45). Closure days, which need the trigger in effect during closed hours and fall inside the program's
  season, number {", ".join(str(cy.get(y, 0)) for y in (2021, 2022, 2023, 2024))}: 2021 counts only the pilot from July 16, and 2022
  loses July 17, when the warning expired at 2&nbsp;a.m. An independent check: an ABC15 report at the end of the 2021 pilot (found by Yun-Peng Lu) said
  the trails closed on eight days with no mountain rescues during closure hours, which matches the {cy.get(2021, 0)} pilot closure days
  and zero closed-hour rescues here. A watch later upgraded to a warning is stored with its end before its
  start; it has no in-effect time of its own, and in this zone every 2021 pilot watch was upgraded to a warning for the same
  days, so watches add no closure days unless a closure began the day a watch was issued. Daily highs: <a href="https://open-meteo.com/">Open-Meteo</a> historical reanalysis at Sky Harbor.
  Trails are assigned from the dispatch address, so unlocated calls count too. The "before" years include 2020, when
  trail use changed with the pandemic.</p>
</div></section>
"""


def write_index(cities: list[tuple[dict, dict, dict]], updated: str, heat_report: dict | None = None,
                nps: dict | None = None) -> None:
    heat = heat_section(heat_report or {})
    nps_sec, nps_script, nps_css = nps_html(nps, esc, kpi), nps_js(nps), (NPS_CSS if nps else "")
    phx = next(s for c, s, _ in cities if c["key"] == "phoenix")
    # The trend uses every mountain-rescue call, located or not. Located-only counts
    # drift with geocoding success (Cholla's address change in 2022 is one example).
    ann = {int(y): sum(v.values()) for y, v in (heat_report or {}).get("all_by_year", {}).items()} or phx["annual"]
    late = round((ann.get(2023, 0) + ann.get(2024, 0) + ann.get(2025, 0)) / 3)
    drop = round(100 * (1 - late / max(ann.get(2019, 1), 1)))
    echo = next((n for a, n in phx["top_locations"] if a == "49XX E MCDONALD DR"), 0)
    echo_pct = round(100 * echo / max(len(phx["mountain"]), 1))

    tabs = "".join(f'<button role="tab" class="tab{" on" if c["key"] == "phoenix" else ""}" data-city="{c["key"]}" '
                   f'aria-selected="{"true" if c["key"] == "phoenix" else "false"}">{esc(c["name"])}'
                   f'<small>{s["years"][0]}–{s["years"][1]}</small></button>'
                   for c, s, _ in cities)
    panels = "".join(city_panel(c, s, r) for c, s, r in cities)
    compare_rows = "".join(
        f'<tr><td>{esc(c["name"])}, {c["state"]}</td><td>{esc(c["agency"])}</td>'
        f'<td class="num">{s["years"][0]}–{s["years"][1]}</td><td class="num">~{s["per_year"]:,}</td>'
        f'<td>{esc(s["busiest_month"])}</td><td>{esc(s["quietest_month"])}</td><td class="num">{s["weekend_pct"]}%</td></tr>'
        for c, s, _ in cities)
    city_cfg = json.dumps({c["key"]: {"center": c["center"], "zoom": c["zoom"], "name": c["name"]} for c, _, _ in cities})
    series = json.dumps({c["key"]: city_series(c, s["rows"]) for c, s, _ in cities}, separators=(",", ":"))
    colors = json.dumps(COLORS)
    labels = json.dumps(CAT_LABEL)

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ridgeline — Search &amp; rescue at the wildland–urban edge</title>
<meta name="description" content="Mountain, technical and water rescue calls from public fire-department dispatch data in Phoenix, Scottsdale and Boulder, plus National Park Service search-and-rescue incidents by park. Mapped, counted, and explained.">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⛰️</text></svg>">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;800&family=Barlow:wght@400;500&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css">
<style>
  :root {{
    --bg:#0f110e; --panel:#171a15; --panel-2:#1d211b; --line:#2b3027; --text:#e4ddcc; --soft:#b3ab98;
    --muted:#8a8676; --ember:#e8793a; --sand:#c8a96e; --sky:#5aa3c0; --link:#7fb8cf;
  }}
  * {{ box-sizing:border-box; }}
  html {{ -webkit-text-size-adjust:100%; }}
  body {{ margin:0; background:var(--bg); color:var(--text); font:16px/1.6 Barlow, system-ui, sans-serif; }}
  a {{ color:var(--link); text-underline-offset:3px; }}
  a:hover {{ color:var(--ember); }}
  .wrap {{ max-width:1120px; margin:0 auto; padding:0 20px; }}
  .mono {{ font-family:'IBM Plex Mono', ui-monospace, monospace; }}
  .topbar {{ border-bottom:1px solid var(--line); font:500 12px/1 'IBM Plex Mono', monospace; letter-spacing:.06em; }}
  .topbar .wrap {{ display:flex; justify-content:space-between; gap:16px; padding-top:12px; padding-bottom:12px; }}
  .topbar a {{ color:var(--muted); text-decoration:none; }} .topbar a:hover {{ color:var(--ember); }}
  header.hero {{ padding:56px 0 28px; }}
  .eyebrow {{ font:500 12px/1 'IBM Plex Mono', monospace; letter-spacing:.18em; text-transform:uppercase; color:var(--ember); }}
  h1 {{ font:800 clamp(44px,8vw,88px)/.9 'Barlow Condensed', sans-serif; text-transform:uppercase; margin:14px 0 18px; letter-spacing:.01em; }}
  h1 span {{ color:var(--ember); }}
  .dek {{ font-size:clamp(17px,2.2vw,20px); color:var(--soft); max-width:62ch; margin:0; }}
  .meta {{ margin-top:18px; font:400 12px/1.6 'IBM Plex Mono', monospace; color:var(--muted); }}
  h2 {{ font:800 30px/1.05 'Barlow Condensed', sans-serif; text-transform:uppercase; letter-spacing:.02em; margin:0 0 16px; }}
  h3 {{ font:600 20px/1.2 'Barlow Condensed', sans-serif; text-transform:uppercase; letter-spacing:.04em; color:var(--sand); margin:34px 0 12px; }}
  section.block {{ padding:40px 0; border-top:1px solid var(--line); }}
  .findings {{ display:grid; grid-template-columns:repeat(3,1fr); gap:14px; }}
  .finding {{ background:var(--panel); border:1px solid var(--line); border-top:3px solid var(--ember); padding:20px; }}
  .finding .big {{ font:800 44px/1 'Barlow Condensed', sans-serif; color:var(--ember); }}
  .finding p {{ margin:10px 0 0; color:var(--soft); font-size:15px; }}
  .finding b {{ color:var(--text); font-weight:500; }}
  .tabs {{ display:flex; gap:8px; flex-wrap:wrap; margin:0 0 18px; }}
  .tab {{ background:var(--panel); color:var(--soft); border:1px solid var(--line); padding:10px 16px; cursor:pointer;
          font:600 18px/1 'Barlow Condensed', sans-serif; text-transform:uppercase; letter-spacing:.05em; border-radius:2px; }}
  .tab small {{ display:block; font:400 11px/1.4 'IBM Plex Mono', monospace; letter-spacing:0; text-transform:none; color:var(--muted); margin-top:4px; }}
  .tab.on {{ border-color:var(--ember); color:var(--text); box-shadow:inset 0 -3px 0 var(--ember); }}
  .tab:focus-visible {{ outline:2px solid var(--ember); outline-offset:2px; }}
  #map {{ height:520px; border:1px solid var(--line); background:#1b1e19; }}
  .legend {{ display:flex; gap:16px; flex-wrap:wrap; margin:10px 0 0; font:400 12px/1.4 'IBM Plex Mono', monospace; color:var(--muted); }}
  .tog {{ background:var(--panel); border:1px solid var(--line); color:var(--muted); padding:6px 10px; cursor:pointer;
          font:400 12px/1.4 'IBM Plex Mono', monospace; border-radius:2px; }}
  .tog.on {{ color:var(--text); border-color:#4a5143; }}
  .tog b {{ font-weight:500; color:var(--soft); margin-left:4px; }}
  .tog[hidden] {{ display:none; }}
  .legend .lead {{ color:var(--sand); }}
  .tog:not(.on) i {{ opacity:.3; }}
  .tog:focus-visible {{ outline:2px solid var(--ember); outline-offset:2px; }}
  .legend .hint {{ align-self:center; }}
  .legend i, .chip i {{ display:inline-block; width:10px; height:10px; border-radius:50%; margin-right:6px; vertical-align:-1px; }}
  .kpis {{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin:26px 0 14px; }}
  .kpi {{ background:var(--panel); border:1px solid var(--line); padding:16px 18px; }}
  .kpi-v {{ font:800 38px/1 'Barlow Condensed', sans-serif; color:var(--text); }}
  .kpi-l {{ font:500 11px/1.4 'IBM Plex Mono', monospace; letter-spacing:.08em; text-transform:uppercase; color:var(--sand); margin-top:8px; }}
  .kpi-s {{ font-size:13px; color:var(--muted); margin-top:2px; }}
  .chips {{ display:flex; gap:10px; flex-wrap:wrap; margin-bottom:18px; }}
  .chip {{ background:var(--panel-2); border:1px solid var(--line); padding:6px 10px; font-size:14px; color:var(--soft); }}
  .chip b {{ color:var(--text); font-weight:500; margin-left:4px; }}
  .grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; }}
  figure.chart {{ margin:0; background:var(--panel); border:1px solid var(--line); padding:14px 16px 10px; }}
  figure.chart figcaption {{ font:500 11px/1.4 'IBM Plex Mono', monospace; letter-spacing:.08em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }}
  figure.chart svg {{ width:100%; height:150px; display:block; overflow:visible; }}
  svg .axis {{ stroke:var(--line); }}
  svg .lab {{ fill:var(--muted); font:10px 'IBM Plex Mono', monospace; text-anchor:middle; }}
  svg .val {{ fill:var(--soft); font:10px 'IBM Plex Mono', monospace; text-anchor:middle; }}
  .muted-box p {{ color:var(--muted); margin:36px 0; font-size:15px; }}
  table {{ width:100%; border-collapse:collapse; font-size:15px; }}
  th {{ text-align:left; font:500 11px/1.4 'IBM Plex Mono', monospace; letter-spacing:.08em; text-transform:uppercase; color:var(--muted);
        border-bottom:1px solid var(--line); padding:8px 10px; }}
  td {{ border-bottom:1px solid var(--line); padding:9px 10px; vertical-align:top; }}
  td.num, th.num {{ text-align:right; font-family:'IBM Plex Mono', monospace; white-space:nowrap; }}
  .places .loc {{ display:block; }}
  .places .addr {{ display:block; font:400 12px/1.4 'IBM Plex Mono', monospace; color:var(--muted); }}
  .barcell {{ width:34%; }}
  .barcell span {{ display:block; height:8px; margin-top:7px; background:var(--ember); opacity:.8; border-radius:1px; }}
  .source, .muted {{ color:var(--muted); font-size:14px; }}
  .source {{ margin-top:14px; }}
  .prose > p, .prose > .note {{ max-width:72ch; }}
  .prose p {{ color:var(--soft); }}
  .prose b {{ color:var(--text); font-weight:500; }}
  .defs {{ display:grid; grid-template-columns:repeat(2,1fr); gap:14px; }}
  .def {{ background:var(--panel); border:1px solid var(--line); padding:16px 18px; }}
  .def h4 {{ margin:0 0 6px; font:600 17px/1.2 'Barlow Condensed', sans-serif; text-transform:uppercase; letter-spacing:.04em; }}
  .def p {{ margin:0; color:var(--soft); font-size:15px; }}
  .note {{ border-left:3px solid var(--sand); background:var(--panel); padding:16px 20px; }}
  .note p {{ margin:0 0 10px; color:var(--soft); }} .note p:last-child {{ margin:0; }}
  .scroll {{ overflow-x:auto; }}
{nps_css}
  details.more {{ margin-top:18px; background:var(--panel); border:1px solid var(--line); padding:12px 16px; }}
  details.more summary {{ cursor:pointer; font:500 12px/1.4 'IBM Plex Mono', monospace; letter-spacing:.06em; text-transform:uppercase; color:var(--sand); }}
  td small, th small {{ color:var(--muted); font-size:11px; }}
  footer {{ border-top:1px solid var(--line); padding:28px 0 48px; color:var(--muted); font:400 12px/1.8 'IBM Plex Mono', monospace; }}
  .leaflet-popup-content-wrapper, .leaflet-popup-tip {{ background:#1d211b; color:var(--text); border-radius:2px; }}
  .leaflet-popup-content {{ font:14px/1.5 Barlow, sans-serif; margin:12px 14px; }}
  .leaflet-popup-content .k {{ font:500 10px/1.4 'IBM Plex Mono', monospace; letter-spacing:.08em; text-transform:uppercase; color:var(--muted); }}
  .leaflet-container a.leaflet-popup-close-button {{ color:var(--muted); }}
  @media (max-width:860px) {{
    .findings {{ grid-template-columns:1fr; }}
    .kpis {{ grid-template-columns:1fr 1fr; }}
    .grid2, .defs {{ grid-template-columns:1fr; }}
    #map {{ height:400px; }}
  }}
</style>
</head>
<body>
<nav class="topbar"><div class="wrap">
  <a href="https://brooksgroves.com/">&larr; brooksgroves.com</a>
  <span><a href="https://github.com/bdgroves/ridgeline">Code &amp; data on GitHub &nearr;</a></span>
</div></nav>

<header class="hero"><div class="wrap">
  <div class="eyebrow">Ridgeline · Search &amp; rescue at the wildland–urban edge</div>
  <h1>Where the <span>trail</span><br>runs out</h1>
  <p class="dek">Every mountain, technical and water rescue that three fire departments put into their public dispatch data.
  Phoenix, Scottsdale and Boulder, located on a map and counted by year, month and hour, so you can see
  when people get into trouble and at which trailheads. Plus <a href="#parks">every search and rescue logged in the
  national parks</a>, 2013–2021.</p>
  <p class="meta">Updated {esc(updated)} · rebuilt weekly from the source data · all counts are dispatched calls, not confirmed rescues</p>
</div></header>

<main>
<section class="block"><div class="wrap">
  <h2>Three things the data says</h2>
  <div class="findings">
    <div class="finding"><div class="big">1 in {round(100 / max(echo_pct, 1))}</div>
      <p><b>Phoenix mountain rescues start at one trailhead.</b> Echo Canyon on Camelback accounts for {echo:,} of the
      {len(phx['mountain']):,} located calls ({echo_pct}%). Add Piestewa Peak, Pima Canyon and Cholla and you have most of the city.</p></div>
    <div class="finding"><div class="big">&minus;{drop}%</div>
      <p><b>Phoenix rescues are down about a fifth.</b> {ann.get(2019, 0)} mountain-rescue calls in 2019, about {late} a year from 2023 to 2025, counting every call whether or not it could be mapped.
      Heat closures only cover about a month of days a year, so they can't be most of it. <a href="#heat">See the heat analysis</a>.</p></div>
    <div class="finding"><div class="big">3 seasons</div>
      <p><b>Each city has its own calendar.</b> Boulder peaks in July. Scottsdale peaks in February and goes quiet in summer.
      Phoenix barely has a season at all: March and May are as busy as July.</p></div>
  </div>
</div></section>

<section class="block" id="explore"><div class="wrap">
  <h2>Explore a city</h2>
  <div class="tabs" role="tablist">{tabs}</div>
  <div id="map" role="region" aria-label="Map of rescue calls"></div>
  <div class="legend" role="group" aria-label="Show on map">
    <span class="hint lead">Show:</span>
    <button class="tog on" data-cat="mountain" aria-pressed="true"><i style="background:{COLORS['mountain']}"></i>Mountain &amp; technical rescue <b data-count="mountain"></b></button>
    <button class="tog" data-cat="water" aria-pressed="false"><i style="background:{COLORS['water']}"></i>Flood &amp; water rescue <b data-count="water"></b></button>
    <button class="tog" data-cat="search" aria-pressed="false"><i style="background:{COLORS['search']}"></i>Land search <b data-count="search"></b></button>
    <span class="hint">These buttons drive the map, the numbers and the charts below · bigger circle = more calls at that spot</span>
  </div>
  {panels}
</div></section>

<section class="block"><div class="wrap">
  <h2>Side by side</h2>
  <div class="scroll"><table>
    <thead><tr><th>City</th><th>Agency</th><th class="num">Data</th><th class="num">Rescues / yr</th><th>Busiest</th><th>Quietest</th><th class="num">Weekend</th></tr></thead>
    <tbody>{compare_rows}</tbody>
  </table></div>
  <p class="source">Mountain &amp; technical rescues only. "Rescues / yr" averages full calendar years. The cities don't count the same way
  (see <a href="#read">How to read this</a>), so compare shapes, not totals.</p>
</div></section>

{heat}
{nps_sec}

<section class="block" id="read"><div class="wrap">
  <h2>How to read this</h2>
  <div class="defs">
    <div class="def"><h4>What counts as a rescue</h4><p>Each department's own call type. In Phoenix, dispatches
      coded <i>Mountain Rescue</i> (plus a handful of tree and confined-space rescues). In Scottsdale, the department's
      <i>MTNRES</i> code and high-angle rescues. In Boulder, rescue and technical-rescue call types, including Boulder
      units sent into county open space ("auto-aid").</p></div>
    <div class="def"><h4>Dispatched, not completed</h4><p>These are 911 dispatches. Some were cancelled en route or found
      nobody. A call is the unit of count, not a person. One call can be a whole group, and some rescues never
      reach the fire department (sheriff's teams and volunteer groups run many backcountry missions).</p></div>
    <div class="def"><h4>How precise a dot is</h4><p>Phoenix publishes addresses to the hundred block ("49XX E McDonald Dr"),
      so a dot marks the middle of that block, or the intersection named in the call. Scottsdale gives full addresses. Boulder
      gives its own points. When a call only names a preserve, the dot sits at a representative point for that preserve.
      Click any dot to see which of these it is.</p></div>
    <div class="def"><h4>What's missing</h4><p>Calls whose address couldn't be matched aren't on the map, but they're still
      in the counts where noted. In Phoenix, {phx['precision'].get('preserve_centroid', 0):,} located calls sit at a preserve's
      representative point rather than a street address. Scottsdale's data starts in December 2022; Boulder's in 2015.</p></div>
  </div>
</div></section>

<section class="block"><div class="wrap prose">
  <h2>A correction</h2>
  <div class="note">
    <p>An earlier version of this page showed 2,263 Phoenix incidents, led by 1,120 "crisis care" calls clustered at
    the preserve edges, plus a wildland-fire cluster. <b>Both were artifacts of a bug.</b> The code that placed
    preserve-name addresses at a preserve also matched street names, so calls on Camelback Road, McDowell Road and
    South Mountain Avenue were pulled in and pinned to preserves up to 20 miles away.</p>
    <p>A second problem hid most of the real rescues: Phoenix's hundred-block addresses weren't being matched, so only
    474 of 1,619 mountain rescues made the map. Both are fixed. Preserve matching now requires the place name, not
    the street; hundred blocks become their midpoint; and every dot records how precise it is. Mapped Phoenix mountain
    rescues went from 474 to {phx['by_cat'].get('mountain', 0):,}. The full write-up is in the
    <a href="https://github.com/bdgroves/ridgeline#corrections">README</a>.</p>
  </div>
</div></section>

<section class="block"><div class="wrap prose">
  <h2>Data &amp; method</h2>
  <p>Each week a GitHub Action downloads every city's public dispatch data, keeps the rescue call types, and
  geocodes addresses with the Maricopa County geocoder, caching every answer so later runs only look up new
  addresses. It then writes one GeoJSON file per city and rebuilds this page. Nothing here is modeled or estimated;
  every number is a count of calls in those files.</p>
  <p>Sources:
  {' · '.join(f'<a href="{esc(c["source_url"])}">{esc(c["name"])}</a> ({esc(c["license"])})' for c, _, _ in cities)}.
  Geocoding by <a href="https://gis.maricopa.gov/">Maricopa County GIS</a>. Basemap &copy; Esri.</p>
  <p>Methods, limitations and code: <a href="https://github.com/bdgroves/ridgeline">github.com/bdgroves/ridgeline</a>.
  The Phoenix view with trails, trailheads and preserve boundaries is on the
  <a href="map.html">detailed Phoenix map</a>.</p>
</div></section>
</main>

<footer><div class="wrap">
  Ridgeline · built by <a href="https://brooksgroves.com/">Brooks Groves</a> · public fire-department data, mapped as published ·
  questions or corrections: contact@brooksgroves.com
</div></footer>

<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<script>
(function() {{
  const CITIES = {city_cfg};
  const COLORS = {colors};
  const LABELS = {labels};
  const PREC = {{hundred_block:'Middle of the hundred block', intersection:'Intersection named in the call',
                address:'Street address', preserve_centroid:'Preserve (representative point)',
                published_point:'Point published by the city'}};
  const map = L.map('map', {{scrollWheelZoom:false}}).setView(CITIES.phoenix.center, CITIES.phoenix.zoom);
  L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{{z}}/{{y}}/{{x}}',
    {{maxZoom:16, attribution:'Tiles &copy; Esri'}}).addTo(map);
  L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Reference/MapServer/tile/{{z}}/{{y}}/{{x}}',
    {{maxZoom:16}}).addTo(map);
  const layers = {{}}, points = {{}};

  // Frame the calls actually shown. The outer 3% on each side is trimmed so a
  // few mutual-aid calls across the valley don't zoom the whole map out.
  function fit(key) {{
    const all = [];
    for (const c of visible) all.push(...((points[key] || {{}})[c] || []));
    if (all.length < 3) {{ map.setView(CITIES[key].center, CITIES[key].zoom); return; }}
    const q = (arr, p) => arr[Math.min(arr.length - 1, Math.max(0, Math.round(p * (arr.length - 1))))];
    const ys = all.map(p => p[0]).sort((a, b) => a - b), xs = all.map(p => p[1]).sort((a, b) => a - b);
    map.fitBounds([[q(ys, .03), q(xs, .03)], [q(ys, .97), q(xs, .97)]], {{padding:[28, 28], maxZoom:13}});
  }}
  const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c]));

  function build(key, gj) {{
    // Group calls sharing a point so stacked dots become one sized circle.
    const groups = new Map();
    for (const f of gj.features) {{
      const p = f.properties;
      let cat = p.category;
      if (!cat) {{
        const t = (p.incident_type || '').toLowerCase();
        cat = /mountain rescue|tree rescue|confined space|rescue service/.test(t) ? 'mountain'
            : /water rescue|swift water|flooding|stranded/.test(t) ? 'water' : null;
      }}
      if (!cat) continue;
      const [x, y] = f.geometry.coordinates;
      const k = cat + '|' + x + '|' + y;
      if (!groups.has(k)) groups.set(k, {{x, y, cat, n:0, p, first:p.date, last:p.date}});
      const g = groups.get(k); g.n++;
      if (p.date < g.first) g.first = p.date; if (p.date > g.last) g.last = p.date;
    }}
    const lg = {{mountain:L.layerGroup(), water:L.layerGroup(), search:L.layerGroup()}};
    const pts = {{mountain:[], water:[], search:[]}};
    const sorted = [...groups.values()].sort((a, b) => (a.cat === 'mountain') - (b.cat === 'mountain') || b.n - a.n);
    for (const g of sorted) {{
      const r = Math.min(4 + Math.sqrt(g.n) * 2.2, 26);
      L.circleMarker([g.y, g.x], {{radius:r, color:COLORS[g.cat], weight:1, fillColor:COLORS[g.cat], fillOpacity:.55}})
        .bindPopup(`<div class="k">${{esc(LABELS[g.cat])}}</div><b>${{g.n.toLocaleString()}} call${{g.n > 1 ? 's' : ''}}</b>`
          + (g.p.location_name ? `<br>${{esc(g.p.location_name)}}` : '')
          + `<br><span class="k">${{esc(g.first === g.last ? g.first : g.first + ' → ' + g.last)}}</span>`
          + `<br><span class="k">Location: ${{esc(PREC[g.p.precision] || g.p.precision || 'not recorded')}}</span>`)
        .addTo(lg[g.cat]);
      for (let i = 0; i < g.n; i++) pts[g.cat].push([g.y, g.x]);
    }}
    layers[key] = lg;
    points[key] = pts;
  }}

  let current = 'phoenix';
  const visible = new Set(['mountain']);
  function draw() {{
    for (const k in layers) for (const c in layers[k]) map.removeLayer(layers[k][c]);
    if (layers[current]) for (const c of visible) layers[current][c].addTo(map);
  }}
  document.querySelectorAll('.tog').forEach(b => b.addEventListener('click', () => {{
    const c = b.dataset.cat, on = !visible.has(c);
    if (!on && visible.size === 1) return;          // keep at least one category on
    on ? visible.add(c) : visible.delete(c);
    b.classList.toggle('on', on); b.setAttribute('aria-pressed', on);
    draw(); fit(current); panel(current);
  }}));

  // ── charts, numbers and tables follow the same buttons ──
  const SERIES = {series};
  const ORDER = ['mountain', 'water', 'search'];
  const MONTHS = ['J','F','M','A','M','J','J','A','S','O','N','D'];
  const MONTHS_L = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  const DAYS = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
  const fmt = n => n.toLocaleString('en-US');

  function bars(parts, labels, opts = {{}}) {{
    // parts: [{{color, values}}] stacked bottom-up in ORDER
    const n = labels.length, w = 600, H = 150, pb = 22, pt = 18, h = H - pb - pt;
    const gap = n < 16 ? 4 : 2, bw = (w - gap * (n - 1)) / n;
    const tot = labels.map((_, i) => parts.reduce((s, p) => s + p.values[i], 0));
    const vmax = Math.max(1, ...tot);
    let out = `<svg viewBox="0 0 ${{w}} ${{H}}" role="img" aria-label="${{esc(opts.title || '')}}" preserveAspectRatio="none">`
            + `<line x1="0" y1="${{pt + h}}" x2="${{w}}" y2="${{pt + h}}" class="axis"/>`;
    for (let i = 0; i < n; i++) {{
      const x = i * (bw + gap); let y = pt + h;
      const op = (opts.faded || []).includes(i) ? .35 : 1;
      for (const p of parts) {{
        const bh = h * p.values[i] / vmax; y -= bh;
        if (bh > 0) out += `<rect x="${{x.toFixed(1)}}" y="${{y.toFixed(1)}}" width="${{bw.toFixed(1)}}" height="${{bh.toFixed(1)}}" fill="${{p.color}}" opacity="${{op}}"><title>${{esc(labels[i])}}: ${{fmt(p.values[i])}}</title></rect>`;
      }}
      if (opts.values !== false && n <= 14 && tot[i]) out += `<text x="${{(x + bw / 2).toFixed(1)}}" y="${{(y - 5).toFixed(1)}}" class="val">${{fmt(tot[i])}}</text>`;
      if (i % (opts.every || 1) === 0) out += `<text x="${{(x + bw / 2).toFixed(1)}}" y="${{H - 6}}" class="lab">${{esc(labels[i])}}</text>`;
    }}
    return out + '</svg>';
  }}

  function panel(key) {{
    const S = SERIES[key], sec = document.getElementById('city-' + key);
    if (!S || !sec) return;
    const cats = ORDER.filter(c => visible.has(c) && S.cats[c]);
    document.querySelectorAll('.tog').forEach(b => {{
      const c = S.cats[b.dataset.cat];
      b.hidden = !c;
      b.querySelector('[data-count]').textContent = c ? fmt(c.total) : '';
    }});
    const sum = f => {{ const len = S.cats[cats[0]] ? S.cats[cats[0]][f].length : 0;
      return Array.from({{length: len}}, (_, i) => cats.reduce((s, c) => s + S.cats[c][f][i], 0)); }};
    const parts = f => cats.map(c => ({{color: COLORS[c], values: S.cats[c][f]}}));
    const PLURAL = {{mountain:'Mountain & technical rescues', water:'Flood & water rescues', search:'Land searches'}};
    const SHORT = {{mountain:'mountain', water:'water', search:'search'}};
    const what = cats.length === 1 ? PLURAL[cats[0]] : (cats.map(c => SHORT[c]).join(' + ') + ' calls').replace(/^./, m => m.toUpperCase());
    if (!cats.length) return;

    const annual = sum('annual'), months = sum('months'), wk = cats.reduce((s, c) => s + S.cats[c].weekend, 0);
    const total = cats.reduce((s, c) => s + S.cats[c].total, 0);
    const full = S.years.map((y, i) => [y, annual[i]]).filter(([y]) => y >= S.full[0] && y <= S.full[1]);
    const perYear = Math.round(full.reduce((s, [, v]) => s + v, 0) / Math.max(1, full.length));
    const busiest = months.indexOf(Math.max(...months)), quietest = months.indexOf(Math.min(...months));
    const faded = S.years.map((y, i) => (y < S.full[0] || y > S.full[1]) ? i : -1).filter(i => i >= 0);

    sec.querySelector('[data-part=kpis]').innerHTML =
      `<div class="kpi"><div class="kpi-v">${{fmt(total)}}</div><div class="kpi-l">${{esc(what)}} mapped</div><div class="kpi-s">${{S.years[0]}}–${{S.years[S.years.length - 1]}}</div></div>`
    + `<div class="kpi"><div class="kpi-v">~${{fmt(perYear)}}</div><div class="kpi-l">Per full year</div><div class="kpi-s">${{S.full[0]}}–${{S.full[1]}} average</div></div>`
    + `<div class="kpi"><div class="kpi-v">${{MONTHS_L[busiest]}}</div><div class="kpi-l">Busiest month</div><div class="kpi-s">quietest: ${{MONTHS_L[quietest]}}</div></div>`
    + `<div class="kpi"><div class="kpi-v">${{(100 * wk / Math.max(1, total)).toFixed(1)}}%</div><div class="kpi-l">On weekends</div><div class="kpi-s">2 of 7 days = 29%</div></div>`;

    const cap = (el, text) => el.querySelector('figcaption').textContent = text;
    const A = sec.querySelector('[data-part=annual]');
    A.innerHTML = `<figcaption></figcaption>` + bars(parts('annual'), S.years.map(String), {{faded, every: S.years.length > 12 ? 2 : 1, title: what + ' per year'}});
    cap(A, what + ' per year' + (faded.length ? ' · faded = partial year' : ''));
    const M = sec.querySelector('[data-part=months]');
    M.innerHTML = `<figcaption></figcaption>` + bars(parts('months'), MONTHS, {{title: what + ' by month'}});
    cap(M, 'By month, all years');
    const D = sec.querySelector('[data-part=weekdays]');
    D.innerHTML = `<figcaption></figcaption>` + bars(parts('weekdays'), DAYS, {{title: what + ' by weekday'}});
    cap(D, 'By day of week');
    const Hh = sec.querySelector('[data-part=hours]');
    if (S.has_hours) {{
      Hh.classList.remove('muted-box');
      Hh.innerHTML = `<figcaption></figcaption>` + bars(parts('hours'), [...Array(24).keys()].map(h => String(h).padStart(2, '0')), {{every: 3, values: false, title: what + ' by hour'}});
      cap(Hh, 'By hour of the call');
    }}

    const merged = new Map();
    for (const c of cats) for (const [label, addr, n] of S.cats[c].top) {{
      const k = label + '|' + addr; merged.set(k, [label, addr, (merged.get(k) || [0, 0, 0])[2] + n]);
    }}
    const top = [...merged.values()].sort((a, b) => b[2] - a[2]).slice(0, 8);
    const P = sec.querySelector('[data-part=places]');
    const note = key === 'boulder' ? P.querySelector('.source') : null;
    P.innerHTML = top.length ? `<table class="places"><thead><tr><th>Dispatch location</th><th class="num">Calls</th><th></th></tr></thead><tbody>`
      + top.map(([l, a, n]) => `<tr><td><span class="loc">${{esc(l)}}</span>${{a ? `<span class="addr">${{esc(a)}}</span>` : ''}}</td><td class="num">${{fmt(n)}}</td><td class="barcell"><span style="width:${{(100 * n / top[0][2]).toFixed(0)}}%"></span></td></tr>`).join('')
      + '</tbody></table>' : '<p class="muted">No locations recorded.</p>';
    if (note) P.appendChild(note);
  }}
  function show(key) {{
    current = key;
    // a category the city doesn't have (land search outside Scottsdale) can't be the only one on
    if (![...visible].some(c => SERIES[key].cats[c])) {{ visible.clear(); visible.add('mountain');
      document.querySelectorAll('.tog').forEach(b => {{ const on = b.dataset.cat === 'mountain'; b.classList.toggle('on', on); b.setAttribute('aria-pressed', on); }}); }}
    draw(); panel(key);
    fit(key);
    document.querySelectorAll('.city').forEach(s => s.hidden = s.dataset.city !== key);
    document.querySelectorAll('.tab').forEach(t => {{
      const on = t.dataset.city === key; t.classList.toggle('on', on); t.setAttribute('aria-selected', on);
    }});
  }}
  document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', () => show(t.dataset.city)));

  Promise.all(Object.keys(CITIES).map(k =>
    fetch(`data/${{k}}_sar_incidents.geojson`).then(r => r.json()).then(gj => build(k, gj)).catch(() => null)
  )).then(() => show(current));
{nps_script}
}})();
</script>
</body>
</html>
"""
    (SITE_DIR / "index.html").write_text(page, encoding="utf-8")


def main() -> None:
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    (SITE_DIR / "data").mkdir(exist_ok=True)
    reports = {"phoenix": load_json("geocode_report.json"),
               "scottsdale": load_json("scottsdale_report.json"),
               "boulder": load_json("boulder_report.json")}
    built = []
    for city in CITIES:
        rows = load_city(city)
        if not rows:
            print(f"  skip {city['name']}: no data")
            continue
        shutil.copy2(EXT_DIR / f"{city['key']}_sar_incidents.geojson",
                     SITE_DIR / "data" / f"{city['key']}_sar_incidents.geojson")
        s = city_stats(city, rows)
        built.append((city, s, reports.get(city["key"], {})))
        print(f"  {city['name']}: {len(s['mountain']):,} mountain/technical · {len(rows):,} total")
    updated = datetime.now(timezone.utc).strftime("%B %-d, %Y")
    nps = nps_data(EXT_DIR, SITE_DIR)
    if nps:
        print(f"  National parks: {nps['total']:,} incidents · {nps['n_parks']} parks")
    write_index(built, updated, load_json("heat_report.json"), nps)
    print(f"  ✓ site/index.html")


if __name__ == "__main__":
    main()
