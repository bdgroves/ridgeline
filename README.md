```
╔══════════════════════════════════════════════════════════════════════════════╗
║  ▶ RIDGELINE // WUI SAR ANALYSIS                              SYSTEM ACTIVE  ║
║  PHOENIX · MARICOPA COUNTY · ARIZONA                       STATUS: DEPLOYED  ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

# RIDGELINE

**[bdgroves.github.io/ridgeline](https://bdgroves.github.io/ridgeline)**

Search & Rescue call volume at the wildland-urban interface · Phoenix · Arizona · 2019–2025

---

> *The mountain is right there. The wash is right there.*
> *The trail runs out. The signal drops. The sun goes down.*
> *And someone who left the house to walk the dog is now a SAR call.*

Phoenix is built directly against some of the most unforgiving terrain in North America. Camelback Mountain, Piestewa Peak, South Mountain, and the McDowell Sonoran Preserve are **inside city limits**. The wildland-urban interface here isn't a distant boundary on a map — it's a sidewalk edge. A trailhead parking lot. The end of a cul-de-sac where the pavement stops and the desert starts.

The result is a SAR load unlike anywhere else in the country. Mountain rescues happen in city parks. Flash floods strand people in wash corridors that bisect suburban neighborhoods. Heat casualties collapse on trails that start in asphalt parking lots. And quietly, largely invisible in the annual rescue statistics, behavioral health crisis calls have become the single largest dispatch category at the WUI edge — more than mountain rescues, more than flood calls, more than wildland fire.

**RIDGELINE maps and quantifies all of it. From real Phoenix Fire Department dispatch data.**

---

```
┌─────────────────────────────────────────────────────────────────────┐
│  PHOENIX WUI SAR — 2019-2025 · 2,263 STRICT SAR INCIDENTS          │
│                                                                     │
│  CRISIS / BEHAVIORAL HEALTH  ████████████████████████  1,120       │
│  MOUNTAIN RESCUE             ████████████░░░░░░░░░░░░    480       │
│  FLASH FLOOD STRANDED        ████████████░░░░░░░░░░░░    477       │
│  WILDLAND FIRE               ████░░░░░░░░░░░░░░░░░░░░    186       │
│                                                                     │
│  36.9% weekend calls  ·  86,168 total incidents processed          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## THE STORY IN THE DATA

**Crisis / Behavioral Health — 1,120 calls (49%)**
The largest single cluster. Behavioral health dispatches at WUI-adjacent addresses, concentrated along the Arizona Canal corridor and the desert preserve edges where encampments form. This isn't a fire department problem. It's a social services and land management problem that shows up in the 911 queue.

**Mountain Rescue — 480 calls (21%)**
Camelback Mountain (Echo Canyon + Cholla trailheads), South Mountain (10200 S Central Ave — the park entrance), North Mountain, Piestewa Peak, McDowell Sonoran Preserve. Daytime, weekends, summer heat. Recreational users underestimating desert terrain. January through December — Camelback doesn't have an off season.

**Flash Flood Stranded — 477 calls (21%)**
The canal system. Phoenix has 131 miles of irrigation canals running through the metro. During monsoon season (July–September) they fill fast and people get caught — in cars, on foot, on bikes. The dispatch pattern follows the drainage geography of the city, not the mountain edges.

**Wildland Fire — 186 calls (8%)**
Grass fires, brush fires, field fires along the desert fringe. The west metro edge where agricultural land meets open desert. Seasonal, wind-driven, fast-moving.

---

## THE MAP

**[bdgroves.github.io/ridgeline/map.html](https://bdgroves.github.io/ridgeline/map.html)**

Interactive folium map — click any dot for a full sitrep card with date, time, nature code, and behavioral cluster. Toggle layers to isolate each cluster. Switch between Dark CARTO, OpenStreetMap, and Satellite.

```
LAYER CONTROL
  Phoenix Mountain Preserves    park boundaries (tan overlay)
  Trailheads                    Maricopa County access points
  Incident Heatmap              density overlay, all incidents
  Recreational - Underequipped  orange  mountain/technical rescue
  Flash Flood Stranded          blue    water rescue + flooding
  Crisis / Behavioral Health    grey    crisis care dispatches
  Wildland Fire                 red     grass/brush/field fire
```

---

## DATA

**86,168 Phoenix Fire Department incidents · 2019–2025**
Source: [phoenixopendata.com](https://www.phoenixopendata.com) · Creative Commons Attribution
82,074 geocoded · Maricopa County geocoder · 2,263 strict SAR incidents mapped

```
NATURE CODES IN DATASET (top SAR-relevant)
  mountain rescue              1,619
  check flooding condition     2,394
  crisis care                    935
  water rescue                   284
  police crisis care             185
  grass / brush / field fire     185
  confined space rescue           33
  tree rescue                     28
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

[ ] Weather correlation — heat index x rescue volume regression
[ ] Camelback closure analysis — did hot weather closures reduce rescues?
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
