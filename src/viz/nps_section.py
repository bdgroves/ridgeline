"""
ridgeline / src / viz / nps_section.py

The National Parks section of the Ridgeline page, from:
    data/external/nps_sar_incidents.csv   (NPS FOIA SAR incident lists)
    data/external/nps_visitation.csv      (NPS IRMA recreation visits)
    data/external/nps_parks.geojson       (NPS boundaries, one feature per park)

The incident lists give park and date only, so the map shows one marker per
park and the park view is a calendar, not a trail map. 2013-2015 look like the
reporting system being phased in and 2021 stops in August, so comparisons use
the five complete, fully reported years, 2016-2020.
"""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

YEARS = list(range(2013, 2022))
FULL = (2016, 2020)
MIN_VISITS_FOR_RATE = 5_000_000   # over the five full years (1M a year) before a rate is ranked


def nps_data(ext: Path, site: Path) -> dict | None:
    inc_p, vis_p, geo_p = ext / "nps_sar_incidents.csv", ext / "nps_visitation.csv", ext / "nps_parks.geojson"
    if not inc_p.exists():
        return None
    with inc_p.open() as f:
        rows = list(csv.DictReader(f))
    visits = defaultdict(dict)
    if vis_p.exists():
        with vis_p.open() as f:
            for r in csv.DictReader(f):
                visits[r["park"]][int(r["year"])] = int(r["visits"])
    geo = {}
    out_dir = site / "data" / "nps"
    out_dir.mkdir(parents=True, exist_ok=True)
    if geo_p.exists():
        for f in json.loads(geo_p.read_text())["features"]:
            p = f["properties"]
            geo[p["park"]] = p
            (out_dir / f"{p['park']}.geojson").write_text(json.dumps(f, separators=(",", ":")))

    if geo_p.exists():
        _write_overview(json.loads(geo_p.read_text())["features"], site / "data" / "nps_overview.geojson")

    parks: dict[str, dict] = {}
    nat_years, nat_months, nat_wk = Counter(), Counter(), Counter()
    no_park = 0
    for r in rows:
        d = date.fromisoformat(r["date"])
        code = r["park"]
        if not code:
            no_park += 1
        if d.year in YEARS:
            nat_years[d.year] += 1
        if FULL[0] <= d.year <= FULL[1]:
            nat_months[d.month] += 1
            nat_wk[d.weekday()] += 1
        if not code:
            continue
        p = parks.setdefault(code, {"years": Counter(), "months": Counter(), "wk": Counter(),
                                    "types": Counter(), "region": Counter(), "total": 0,
                                    "first": r["date"], "last": r["date"]})
        p["first"], p["last"] = min(p["first"], r["date"]), max(p["last"], r["date"])
        p["total"] += 1
        p["region"][r["region"]] += 1
        if d.year in YEARS:
            p["years"][d.year] += 1
        if FULL[0] <= d.year <= FULL[1]:
            p["months"][d.month] += 1
            p["wk"][d.weekday()] += 1
        t = r["type"].lower()
        p["types"]["water" if "water" in t else "land" if "land" in t else "sar"] += 1

    out = {}
    for code, p in parks.items():
        full = sum(p["years"][y] for y in range(FULL[0], FULL[1] + 1))
        v = sum(visits[code].get(y, 0) for y in range(FULL[0], FULL[1] + 1))
        g = geo.get(code, {})
        out[code] = {
            "name": g.get("name") or code,
            "region": p["region"].most_common(1)[0][0] if p["region"] else "",
            "lat": g.get("lat"), "lon": g.get("lon"), "geom": code in geo,
            "total": p["total"], "full": full, "first": p["first"], "last": p["last"],
            "years": [p["years"][y] for y in YEARS],
            "months": [p["months"][m] for m in range(1, 13)],
            "wk": [p["wk"][i] for i in range(7)],
            "types": [p["types"]["sar"], p["types"]["land"], p["types"]["water"]],
            "visits": v,
            "rate": round(full / v * 1e6, 1) if v >= MIN_VISITS_FOR_RATE else None,
        }
    ranked = sorted(out, key=lambda k: -out[k]["full"])
    full_total = sum(nat_years[y] for y in range(FULL[0], FULL[1] + 1))
    top3 = sum(out[k]["full"] for k in ranked[:3])
    return {
        "parks": out, "years": YEARS, "full": list(FULL),
        "national": {"years": [nat_years[y] for y in YEARS],
                     "months": [nat_months[m] for m in range(1, 13)],
                     "wk": [nat_wk[i] for i in range(7)]},
        "total": len(rows), "no_park": no_park, "n_parks": len(out),
        "full_total": full_total, "top3": ranked[:3], "top3_share": round(100 * top3 / max(full_total, 1)),
        "last_date": max(r["date"] for r in rows),
        "default": "YOSE" if "YOSE" in out else ranked[0],
    }


def _write_overview(features: list, out: Path) -> None:
    """
    A light copy of every park outline for the national map: coordinates snapped
    to 0.02 degrees (about 2 km), holes and specks dropped. The detailed outline
    is still loaded when a park is picked.
    """
    feats = []
    for f in features:
        parts = []
        for poly in f["geometry"]["coordinates"]:
            ring, prev = [], None
            for x, y in poly[0]:
                pt = (round(x / 0.02) * 0.02, round(y / 0.02) * 0.02)
                pt = (round(pt[0], 2), round(pt[1], 2))
                if pt != prev:
                    ring.append(pt); prev = pt
            if len(ring) < 4:
                continue
            xs, ys = [p[0] for p in ring], [p[1] for p in ring]
            parts.append((max(max(xs) - min(xs), max(ys) - min(ys)), [[list(p) for p in ring]]))
        if not parts:
            continue
        big = max(parts)[0]
        keep = [pp for span, pp in parts if span >= 0.06 or span == big]
        feats.append({"type": "Feature", "properties": {"park": f["properties"]["park"]},
                      "geometry": {"type": "MultiPolygon", "coordinates": keep}})
    out.write_text(json.dumps({"type": "FeatureCollection", "features": feats}, separators=(",", ":")))


def _name(p: dict) -> str:
    return (p["name"] or "").replace(" National Park", " NP").replace(" National Recreation Area", " NRA")


def nps_html(d: dict | None, esc, kpi) -> str:
    if not d:
        return ""
    P = d["parks"]
    months_l = ["January", "February", "March", "April", "May", "June", "July", "August",
                "September", "October", "November", "December"]
    nm = d["national"]["months"]
    busiest = months_l[nm.index(max(nm))]
    wk = d["national"]["wk"]
    weekend = round(100 * (wk[5] + wk[6]) / max(sum(wk), 1), 1)
    top = [f"{esc(_name(P[k]))} ({P[k]['full'] // 5:,}/yr)" for k in d["top3"]]
    options = "".join(
        f'<option value="{k}"{" selected" if k == d["default"] else ""}>{esc(_name(P[k]))} · {P[k]["full"]:,}</option>'
        for k in sorted(P, key=lambda k: -P[k]["full"]) if P[k]["full"] > 0)
    last = date.fromisoformat(d["last_date"]).strftime("%B %Y")
    return f"""
<section class="block" id="parks"><div class="wrap">
  <h2>National parks</h2>
  <div class="prose">
  <p>The National Park Service released lists of every search-and-rescue incident its rangers logged from 2013 to
  {esc(last)}: {d["total"]:,} incidents across {d["n_parks"]} parks. Each record gives the park, the date and the kind
  of incident, but <b>not where in the park it happened</b>, so the map has one circle per park rather than one dot per
  rescue. Pick a park to see its calendar.</p>
  </div>
  <div class="kpis">
    {kpi(f'{d["full_total"] // 5:,}', "Incidents a year", f'{d["full"][0]}–{d["full"][1]} average, all parks')}
    {kpi(f'{d["top3_share"]}%', "In just three parks", " · ".join(esc(_name(P[k]).replace(" NP", "").replace(" NRA", "")) for k in d["top3"]))}
    {kpi(esc(busiest), "Busiest month", "all parks, 2016–2020")}
    {kpi(f"{weekend}%", "On weekends", "2 of 7 days = 29%")}
  </div>
  <div class="legend" role="group" aria-label="Size circles by">
    <span class="hint lead">Circle size:</span>
    <button class="ntog on" data-mode="count" aria-pressed="true">Incidents per year</button>
    <button class="ntog" data-mode="rate" aria-pressed="false">Per million visits</button>
    <button class="ntog on" data-mode="bounds" aria-pressed="true"><i style="background:#8fb573"></i>Park boundaries</button>
    <button class="ntog" data-mode="reset" aria-pressed="false">&#8634; All parks</button>
    <span class="hint">Click a park's circle or boundary for its numbers, or choose one below</span>
  </div>
  <div id="npsmap" role="region" aria-label="Map of national park search-and-rescue incidents"></div>

  <div class="parkbar">
    <label for="parkpick">Park</label>
    <select id="parkpick">{options}</select>
  </div>
  <div id="parkpanel">
    <div class="kpis" data-np="kpis"></div>
    <div class="grid2">
      <figure class="chart" data-np="years"><figcaption>Incidents per year · faded = partial reporting</figcaption></figure>
      <figure class="chart" data-np="months"><figcaption>By month, 2016–2020</figcaption></figure>
      <figure class="chart" data-np="wk"><figcaption>By day of week, 2016–2020</figcaption></figure>
      <figure class="chart" data-np="types"><figcaption>Kind of incident</figcaption></figure>
    </div>
  </div>

  <h3>Where rescues happen most</h3>
  <div class="legend" role="group" aria-label="Rank parks by">
    <span class="hint lead">Rank by:</span>
    <button class="rtog on" data-rank="count" aria-pressed="true">Incidents</button>
    <button class="rtog" data-rank="rate" aria-pressed="false">Per million visits</button>
  </div>
  <div class="scroll"><table class="places" id="parkrank">
    <thead><tr><th>Park</th><th class="num">Incidents / yr</th><th class="num">Visits / yr</th><th class="num">Per million visits</th><th></th></tr></thead>
    <tbody></tbody>
  </table></div>
  <p class="source">Source: <a href="https://www.nps.gov/aboutus/foia/foia-frd.htm">NPS FOIA reading room</a>, SAR incident
  lists 2013–2018 and 2019–2020 (the second runs to {esc(last)}); visits from
  <a href="https://irma.nps.gov/Stats/">NPS visitor use statistics</a>; boundaries from the NPS Land Resources Division.
  Averages and rates use {d["full"][0]}–{d["full"][1]}: fewer incidents were logged in 2013–2015 as the reporting system came
  into use, and 2021 is partial. Rates are ranked only for parks with at least a million visits a year. 2020 visits fell with
  the pandemic. {d["no_park"]:,} incidents have no park code and appear only in the national totals. "Search" types
  are recorded separately only in 2013–2018. An incident is one logged event, not one person.</p>
</div></section>
"""


NPS_JS = r"""
  // ── National parks ──
  const NPS = __NPS__;
  const NY = NPS.years, NFULL = NPS.full;
  const nmap = L.map('npsmap', {scrollWheelZoom:false, worldCopyJump:true});
  L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}',
    {maxZoom:12, attribution:'Tiles &copy; Esri · NPS'}).addTo(nmap);
  L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}',
    {maxZoom:12}).addTo(nmap);
  const US = [[24.5, -125], [49.5, -66.5]];
  nmap.fitBounds(US);
  // Every park outline, under the circles. Click one to pick that park.
  nmap.createPane('bounds'); nmap.getPane('bounds').style.zIndex = 350;
  // Zoomed out: a light, simplified copy. Zoomed in (7+): each park in view swaps
  // to its detailed outline, fetched once and cached.
  let bounds = null, showBounds = true;
  const BSTYLE = {color:'#8fb573', weight:.8, opacity:.7, fillColor:'#8fb573', fillOpacity:.14};
  const overview = {}, detail = {}, loading = {};
  const hook = (code, lyr) => {
    const p = NPS.parks[code]; if (!p) return;
    lyr.bindTooltip(esc(short(p.name)), {sticky:true});
    lyr.on('click', e => { pick(code, true); popup(code, e.latlng); });
  };
  fetch('data/nps_overview.geojson').then(r => r.json()).then(gj => {
    bounds = L.featureGroup().addTo(nmap);
    for (const f of gj.features) {
      const lyr = L.geoJSON(f, {pane:'bounds', style:BSTYLE}); hook(f.properties.park, lyr);
      overview[f.properties.park] = lyr; bounds.addLayer(lyr);
    }
    refine();
  }).catch(() => null);
  function refine() {
    if (!bounds || !showBounds) return;
    const close = nmap.getZoom() >= 7, view = nmap.getBounds().pad(.3);
    for (const [code, lyr] of Object.entries(overview)) {
      const d = detail[code];
      const want = close && lyr.getBounds().intersects(view);
      if (want && !d && !loading[code] && NPS.parks[code] && NPS.parks[code].geom) {
        loading[code] = true;
        fetch(`data/nps/${code}.geojson`).then(r => r.json()).then(gj => {
          detail[code] = L.geoJSON(gj, {pane:'bounds', style:BSTYLE}); hook(code, detail[code]); refine();
        }).catch(() => null);
      }
      const useDetail = want && d;
      if (useDetail) { bounds.removeLayer(lyr); bounds.addLayer(d); }
      else { if (d) bounds.removeLayer(d); bounds.addLayer(lyr); }
    }
  }
  nmap.on('zoomend moveend', refine);
  const PK = Object.entries(NPS.parks).filter(([, p]) => p.lat != null && p.full > 0);
  const maxN = Math.max(...PK.map(([, p]) => p.full)), maxR = Math.max(...PK.map(([, p]) => p.rate || 0));
  let nmode = 'count', picked = null, outline = null;
  const nmark = {};
  const nrad = p => nmode === 'count' ? 3 + 24 * Math.sqrt(p.full / maxN) : (p.rate ? 3 + 24 * Math.sqrt(p.rate / maxR) : 2);
  const short = n => n.replace(' National Park', ' NP').replace(' National Recreation Area', ' NRA');
  for (const [k, p] of PK.sort((a, b) => b[1].full - a[1].full)) {
    nmark[k] = L.circleMarker([p.lat, p.lon], {radius: nrad(p), color: COLORS.mountain, weight: 1,
      fillColor: COLORS.mountain, fillOpacity: .5})
      .bindTooltip(`<b>${esc(short(p.name))}</b><br>${fmt(Math.round(p.full / 5))} incidents a year` +
                   (p.rate ? `<br>${p.rate.toFixed(1)} per million visits` : ''), {direction:'top'})
      .on('click', e => { pick(k, true); popup(k, e.latlng); }).addTo(nmap);
  }
  // Click a park: the same kind of popup as the city dots, plus a jump to its charts.
  const RANK = Object.entries(NPS.parks).filter(([, p]) => p.full > 0).sort((a, b) => b[1].full - a[1].full).map(([k]) => k);
  const niceDate = d => new Date(d + 'T12:00').toLocaleDateString('en-US', {month:'short', year:'numeric'});
  function popup(k, latlng) {
    const p = NPS.parks[k]; if (!p) return;
    const bm = p.months.indexOf(Math.max(...p.months)), wkTot = p.wk.reduce((a, b) => a + b, 0);
    const html = `<div class="k">NPS · ${esc(k)} · ${esc(p.region)}</div><b>${esc(p.name)}</b>`
      + `<br>~${fmt(Math.round(p.full / 5))} search &amp; rescue incidents a year`
      + `<br><span class="k">Rank ${RANK.indexOf(k) + 1} of ${RANK.length} parks · ${NFULL[0]}–${NFULL[1]}</span>`
      + (p.rate != null ? `<br>${p.rate.toFixed(1)} per million visits` : (p.visits ? `<br><span class="k">Too few visits to rank a rate</span>` : ''))
      + (p.full ? `<br>Busiest month: ${MONTHS_L[bm]} · weekends ${(100 * (p.wk[5] + p.wk[6]) / Math.max(1, wkTot)).toFixed(0)}%` : '')
      + `<br><span class="k">${fmt(p.total)} logged · ${esc(niceDate(p.first))} → ${esc(niceDate(p.last))}</span>`
      + `<br><a href="#parkpanel" class="k">Year-by-year charts &darr;</a>`;
    L.popup({maxWidth:280, autoPanPadding:[20, 20]}).setLatLng(latlng).setContent(html).openOn(nmap);
  }
  function resize() { for (const [k, p] of PK) nmark[k].setRadius(nrad(p)); }

  function pick(k, fromMap, zoom = true) {
    const p = NPS.parks[k]; if (!p) return;
    picked = k; document.getElementById('parkpick').value = k;
    for (const [kk] of PK) nmark[kk].setStyle(kk === k ? {color:'#fff', weight:2, fillOpacity:.8} : {color:COLORS.mountain, weight:1, fillOpacity:.5});
    if (outline) { nmap.removeLayer(outline); outline = null; }
    if (p.geom) fetch(`data/nps/${k}.geojson`).then(r => r.json()).then(gj => {
      if (picked !== k) return;
      // Not interactive, so the circle and boundary underneath still take clicks.
      outline = L.geoJSON(gj, {interactive:false, style:{color:'#fff', weight:1.5, fill:true, fillColor:COLORS.mountain, fillOpacity:.12}}).addTo(nmap);
      if (zoom && (!fromMap || nmap.getZoom() < 5)) nmap.fitBounds(outline.getBounds(), {padding:[30, 30], maxZoom:9});
    }).catch(() => null);
    const full = p.full, perYr = Math.round(full / 5);
    const bm = p.months.indexOf(Math.max(...p.months)), qm = p.months.indexOf(Math.min(...p.months));
    const wkTot = p.wk.reduce((a, b) => a + b, 0);
    const box = document.getElementById('parkpanel');
    box.querySelector('[data-np=kpis]').innerHTML =
      `<div class="kpi"><div class="kpi-v">~${fmt(perYr)}</div><div class="kpi-l">Incidents a year</div><div class="kpi-s">${esc(short(p.name))}, ${NFULL[0]}–${NFULL[1]}</div></div>`
    + `<div class="kpi"><div class="kpi-v">${p.rate != null ? p.rate.toFixed(1) : '–'}</div><div class="kpi-l">Per million visits</div><div class="kpi-s">${p.visits ? fmt(Math.round(p.visits / 5)) + ' visits a year' : 'no visit counts'}</div></div>`
    + `<div class="kpi"><div class="kpi-v">${full ? MONTHS_L[bm] : '–'}</div><div class="kpi-l">Busiest month</div><div class="kpi-s">${full ? 'quietest: ' + MONTHS_L[qm] : ''}</div></div>`
    + `<div class="kpi"><div class="kpi-v">${wkTot ? (100 * (p.wk[5] + p.wk[6]) / wkTot).toFixed(0) + '%' : '–'}</div><div class="kpi-l">On weekends</div><div class="kpi-s">2 of 7 days = 29%</div></div>`;
    const faded = NY.map((y, i) => (y < NFULL[0] || y > NFULL[1]) ? i : -1).filter(i => i >= 0);
    const put = (part, svg) => { const f = box.querySelector(`[data-np=${part}]`); const cap = f.querySelector('figcaption').outerHTML; f.innerHTML = cap + svg; };
    put('years', bars([{color:COLORS.mountain, values:p.years}], NY.map(String), {faded, title:'Incidents per year'}));
    put('months', bars([{color:COLORS.mountain, values:p.months}], MONTHS, {title:'Incidents by month'}));
    put('wk', bars([{color:COLORS.mountain, values:p.wk}], DAYS, {title:'Incidents by weekday'}));
    put('types', bars([{color:COLORS.search, values:p.types}], ['Search & rescue', 'Land search*', 'Water search*'], {title:'Kind of incident'})
         + '<p class="source" style="margin:6px 0 0">* recorded as separate types only in 2013–2018</p>');
  }
  document.getElementById('parkpick').addEventListener('change', e => pick(e.target.value, false));
  document.querySelectorAll('.ntog').forEach(b => b.addEventListener('click', () => {
    if (b.dataset.mode === 'bounds') {
      const on = !b.classList.contains('on'); b.classList.toggle('on', on); b.setAttribute('aria-pressed', on);
      showBounds = on; if (bounds) { on ? bounds.addTo(nmap) : nmap.removeLayer(bounds); refine(); } return;
    }
    if (b.dataset.mode === 'reset') { if (outline) { nmap.removeLayer(outline); outline = null; } nmap.fitBounds(US); return; }
    nmode = b.dataset.mode; resize();
    document.querySelectorAll('.ntog[data-mode=count], .ntog[data-mode=rate]').forEach(x => {
      const on = x === b; x.classList.toggle('on', on); x.setAttribute('aria-pressed', on); });
  }));

  let rank = 'count';
  function table() {
    const rows = Object.entries(NPS.parks).filter(([, p]) => p.full > 0 && (rank === 'count' || p.rate != null))
      .sort((a, b) => rank === 'count' ? b[1].full - a[1].full : b[1].rate - a[1].rate).slice(0, 15);
    const top = rank === 'count' ? rows[0][1].full : rows[0][1].rate;
    document.querySelector('#parkrank tbody').innerHTML = rows.map(([k, p]) =>
      `<tr data-park="${k}" tabindex="0"><td><span class="loc">${esc(short(p.name))}</span><span class="addr">${k} · ${esc(p.region)}</span></td>`
      + `<td class="num">${fmt(Math.round(p.full / 5))}</td><td class="num">${p.visits ? (p.visits / 5e6).toFixed(1) + 'M' : '–'}</td>`
      + `<td class="num">${p.rate != null ? p.rate.toFixed(1) : '–'}</td>`
      + `<td class="barcell"><span style="width:${(100 * (rank === 'count' ? p.full : p.rate) / top).toFixed(0)}%"></span></td></tr>`).join('');
    document.querySelectorAll('#parkrank tbody tr').forEach(tr => {
      const go = () => { pick(tr.dataset.park, false); document.getElementById('npsmap').scrollIntoView({behavior:'smooth', block:'center'}); };
      tr.addEventListener('click', go); tr.addEventListener('keydown', e => { if (e.key === 'Enter') go(); });
    });
  }
  document.querySelectorAll('.rtog').forEach(b => b.addEventListener('click', () => {
    rank = b.dataset.rank; table();
    document.querySelectorAll('.rtog').forEach(x => { const on = x === b; x.classList.toggle('on', on); x.setAttribute('aria-pressed', on); });
  }));
  table();
  pick(NPS.default, true, false);   // outline the default park, keep the national view
"""


def nps_js(d: dict | None) -> str:
    if not d:
        return ""
    return NPS_JS.replace("__NPS__", json.dumps(d, separators=(",", ":")))


NPS_CSS = """
  #npsmap { height:520px; border:1px solid var(--line); background:#1b1e19; margin-top:10px; }
  .ntog, .rtog { background:var(--panel); border:1px solid var(--line); color:var(--muted); padding:6px 10px; cursor:pointer;
          font:400 12px/1.4 'IBM Plex Mono', monospace; border-radius:2px; }
  .ntog.on, .rtog.on { color:var(--text); border-color:#4a5143; }
  .ntog:focus-visible, .rtog:focus-visible, #parkrank tr:focus-visible { outline:2px solid var(--ember); outline-offset:2px; }
  .parkbar { display:flex; gap:12px; align-items:center; margin:22px 0 0; flex-wrap:wrap; }
  .parkbar label { font:500 11px/1.4 'IBM Plex Mono', monospace; letter-spacing:.08em; text-transform:uppercase; color:var(--sand); }
  .parkbar select { background:var(--panel); color:var(--text); border:1px solid var(--line); padding:8px 10px; font:15px Barlow, sans-serif;
          max-width:100%; border-radius:2px; }
  #parkrank tbody tr { cursor:pointer; }
  #parkrank tbody tr:hover td { background:var(--panel); }
  @media (max-width:860px) { #npsmap { height:400px; } }
"""
