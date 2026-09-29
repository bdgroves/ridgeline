```
╔══════════════════════════════════════════════════════════════════════════════╗
║  ▶ RIDGELINE // WUI SAR ANALYSIS                              SYSTEM ACTIVE  ║
║  PHOENIX · MARICOPA COUNTY · ARIZONA                       STATUS: DEPLOYED  ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

# RIDGELINE

**[brooksgroves.com/ridgeline](https://brooksgroves.com/ridgeline/)**

Search & Rescue call volume at the wildland-urban interface · Phoenix · Arizona · 2019–2025

---

> *The mountain is right there. The wash is right there.*
> *The trail runs out. The signal drops. The sun goes down.*
> *And someone who left the house to walk the dog is now a SAR call.*

Phoenix is built directly against some of the most unforgiving terrain in North America. Camelback Mountain, Piestewa Peak, South Mountain, and the McDowell Sonoran Preserve are **inside city limits**. The wildland-urban interface here isn't a distant boundary on a map — it's a sidewalk edge. A trailhead parking lot. The end of a cul-de-sac where the pavement stops and the desert starts.

The result is a SAR load unlike anywhere else in the country. Mountain rescues happen in city parks. Flash floods strand people in wash corridors that bisect suburban neighborhoods. Heat casualties collapse on trails that start in asphalt parking lots.

**RIDGELINE maps and quantifies all of it. From real Phoenix Fire Department dispatch data.**

---

```
┌─────────────────────────────────────────────────────────────────────┐
│  PHOENIX WUI SAR — 2019-2025 · 2,814 MAPPED SAR INCIDENTS          │
│                                                                     │
│  FLOOD / WATER RESCUE        ████████████████████████  1,650       │
│  MOUNTAIN RESCUE             █████████████████░░░░░░░  1,159       │
│  OTHER (crisis at preserves) ░░░░░░░░░░░░░░░░░░░░░░░░      5       │
│                                                                     │
│  1,159 of 1,619 mountain rescues mapped (72%) · 36.4% weekend      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## THE STORY IN THE DATA

**Mountain Rescue — 1,619 calls, 1,159 mapped**
It's a handful of trailheads. Echo Canyon on Camelback (49XX E McDonald Dr) alone accounts for 310, a fifth of every mountain rescue in the city. Then Piestewa Peak (the park road, logged under both its current name and the old Squaw Peak Dr), Pima Canyon on South Mountain, Cholla on Camelback's east side, and the South Mountain park entrance at S Central Ave.

Two things surprised me. **There is no summer peak.** Rescues run roughly 85–120 a month all year (December is the quiet one); March and May are as busy as July. And **rescues are down about a third**: 206 in 2019, ~130 a year from 2023 on. That drop lines up with Phoenix's heat-triggered trail closures at Echo Canyon and Cholla, which is a hypothesis worth testing, not a finding yet.

**Flood / Water Rescue — 1,650 mapped**
Mostly "check flooding condition" dispatches (1,455) plus water rescues (195). Busiest in the monsoon months, July–September, but present all year: this is a canal and wash city, and the calls follow drainage, not mountains.

---

## CORRECTIONS (September 2026)

Earlier versions of this page reported 2,263 incidents with **Crisis / Behavioral Health (1,120)** as the largest cluster, concentrated "along the preserve edges," plus **Wildland Fire (186)**. Both were artifacts of a geocoding bug, and they're gone:

- The preserve-name fallback matched **street names**. "McDowell", "Camelback" and "South Mountain" matched McDowell Road, Camelback Road and South Mountain Avenue, so ordinary city calls on those arterials were pulled into the dataset and pinned to preserve centroids up to 20 miles away. About 1,560 of the 2,263 old points sat on four such centroids.
- Phoenix publishes addresses masked to the hundred block (`11XX E MCDOWELL RD`) and intersections as `A/B`. The geocoder couldn't score either, so most real mountain rescues silently dropped out: only 474 of 1,619 made the old map.

Fixes: preserve matching now uses patterns that mean the place (Echo Canyon, Piestewa Peak, McDowell Sonoran) and only after the geocoder fails; hundred blocks become the block midpoint; intersections become `A & B`; street-type abbreviations (`PW`, `AV`) are expanded; a near-miss score (70–80) is accepted only when the geocoder's match is on the same street. Every point now records its precision (`hundred_block`, `intersection`, `preserve_centroid`). The match report is committed at `data/external/geocode_report.json`.

---

## THE MAP

**[brooksgroves.com/ridgeline/map.html](https://brooksgroves.com/ridgeline/map.html)**

Interactive folium map — click any dot for a full sitrep card with date, time, nature code, and behavioral cluster. Toggle layers to isolate each cluster. Switch between Dark CARTO, OpenStreetMap, and Satellite.

```
LAYER CONTROL
  Phoenix Mountain Preserves    park boundaries (tan overlay)
  Trailheads                    Maricopa County access points
  Incident Heatmap              density overlay, all incidents
  Recreational - Underequipped  orange  mountain/technical rescue
  Flash Flood Stranded          blue    water rescue + flooding
```

---

## DATA

**Phoenix Fire Department calls for service · 2019–2025**
Source: [phoenixopendata.com](https://www.phoenixopendata.com) · Creative Commons Attribution
5,476 SAR-relevant calls kept (rescue/flood nature codes + calls at preserve addresses) · 3,245 of 4,733 priority calls geocoded (Maricopa County geocoder, cached in `data/external/geocode_cache.csv`) · 2,814 SAR incidents mapped

```
MOUNTAIN RESCUE — HOW THE 1,619 BECOMES 1,159
  calls with nature text "Mountain Rescue"      1,619   (1,619 unique incident IDs)
  geocoded, score ≥ 80                            275
  geocoded, same-street near miss (70–80)         704
  preserve centroid (Echo Canyon, Piestewa…)     180
  not located                                     460
```

---

## QUICK START

```bash
git clone https://github.com/bdgroves/ridgeline.git
cd ridgeline

# Windows PowerShell
iwr -useb https://pixi.sh/install.ps1 | iex
pixi install
```

```bash
pixi run phoenix     # fetch Phoenix Fire 2019-2025 (~86k incidents)
pixi run geocode     # geocode addresses via Maricopa County API
pixi run gis         # fetch park/trail/trailhead GIS layers
pixi run stats       # 10 analysis plots
pixi run map         # build interactive folium map
pixi run build       # assemble GitHub Pages site
pixi run serve       # preview at localhost:8080
```

---

## PROJECT STRUCTURE

```
ridgeline/
├── pixi.toml
├── .github/workflows/deploy.yml      weekly CI/CD → GitHub Pages
│
├── src/
│   ├── ingest/
│   │   ├── fetch_phoenix_fire.py     Phoenix Fire open data 2019-2025
│   │   ├── geocode_phoenix.py        Maricopa County geocoder
│   │   └── fetch_gis.py              park / trail / trailhead GIS layers
│   ├── analysis/
│   │   ├── sar_stats.py              10 behavioral cluster plots
│   │   ├── wui_model.py              RF + logistic rescue prediction
│   │   └── weather_pull.py           Open-Meteo historical join
│   └── viz/
│       ├── build_map.py              interactive folium map
│       └── build_site.py             GitHub Pages assembler
│
├── data/
│   ├── external/                     GIS layers + SAR GeoJSON (tracked)
│   ├── raw/                          source CSVs (gitignored)
│   └── processed/                    parquets (gitignored)
│
└── export_geojson.py                 exports SAR incidents → GeoJSON
```

---

## ROADMAP

```
[x] Phoenix Fire real data — 86k incidents 2019-2025
[x] Geocoding — 82k addresses resolved
[x] 4 real behavioral clusters from actual nature codes
[x] Interactive map — sitrep popups, layer control, OSM/satellite/dark
[x] Maricopa GIS layers — parks, trails, trailheads
[x] 10 analysis plots + rescue prediction model
[x] GitHub Pages CI/CD — auto-deploy on push

[x] Geocoding fix — street-name fallbacks, hundred-block addresses (Sept 2026)
[x] Geocoding in CI with a committed address cache + match report
[ ] Weather correlation — heat index x rescue volume regression
[ ] Camelback closure analysis — did hot weather closures reduce rescues?
    (rescues fell ~35% from 2019 to 2023–25; closures are the obvious suspect)
[ ] Locate the remaining 460 — N 7th St (North Mountain), Invergordon Rd,
    Galvin Pkwy (Papago) don't resolve at the county geocoder
[ ] Full FOIA data — richer incident detail, more clusters
[ ] Los Angeles — LAFD FOIA in progress (Angeles NF / Santa Monica Mtns)
[ ] Portland, OR — Portland Fire & Rescue open data
[ ] Seattle / King County — Sheriff SAR unit FOIA
```

---

## PENDING DATA REQUESTS

```
MCSO     Maricopa County Sheriff SAR log 2019-2024    mcso.org
AZ DEMA  Statewide SAR mission log (~600/yr)          dema.az.gov
LAFD     Incident-level data with nature codes        lacity.nextrequest.com
```

---

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  SOURCES                                                                     ║
║  Phoenix Fire Dept open data · Maricopa County GIS · Open-Meteo             ║
║                                                                              ║
║  STACK                                                                       ║
║  Python · pixi · pandas · geopandas · folium · matplotlib · seaborn         ║
║  scikit-learn · httpx · rich · GitHub Actions · GitHub Pages                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
```
