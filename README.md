```
╔══════════════════════════════════════════════════════════════════════════════╗
║  ▶ RIDGELINE // WUI SAR ANALYSIS                              SYSTEM ACTIVE  ║
║  PHOENIX · MARICOPA COUNTY · ARIZONA                       STATUS: DEPLOYED  ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

# RIDGELINE

**Search & Rescue call volume at the wildland-urban interface**

> *The mountain is right there. The wash is right there.*  
> *The trail runs out. The signal drops. The sun goes down.*  
> *And someone who left the house to walk the dog is now a SAR call.*

Phoenix is built directly against some of the most unforgiving terrain in North America. Camelback Mountain, Piestewa Peak, South Mountain, and the McDowell Sonoran Preserve are **inside city limits**. The wildland-urban interface here isn't a distant boundary — it's a sidewalk edge.

The result is a uniquely dense SAR load. 200+ mountain rescues per year in Phoenix city parks alone. Flash flood strandings in wash corridors that bisect suburban neighborhoods. Heat casualties on trails that start in parking lots.

**RIDGELINE maps and quantifies all of it — from real Phoenix Fire Department dispatch data.**

---

```
┌─────────────────────────────────────────────────────────────┐
│  PHOENIX WUI SAR — 2019-2025                                │
│                                                             │
│  MOUNTAIN RESCUE   ████████████████████░░░░  1,619 calls   │
│  CHECK FLOODING    ████████████░░░░░░░░░░░░  2,394 calls   │
│  WATER RESCUE      ████░░░░░░░░░░░░░░░░░░░░    284 calls   │
│  TECHNICAL RESCUE  ███░░░░░░░░░░░░░░░░░░░░░    ~200 calls  │
│  HEAT / MEDICAL    ██░░░░░░░░░░░░░░░░░░░░░░    ~180 calls  │
└─────────────────────────────────────────────────────────────┘
```

---

## DATA

**86,168 real Phoenix Fire Department incidents · 2019–2025**  
Source: [phoenixopendata.com](https://www.phoenixopendata.com) · Creative Commons Attribution  
82,074 geocoded to street address · Maricopa County geocoder

### Key locations
```
CAMELBACK MOUNTAIN    Echo Canyon + Cholla trailheads  — highest rescue density
SOUTH MOUNTAIN        Largest municipal park in US     — wash + terrain incidents
MCDOWELL SONORAN      Scottsdale WUI edge              — mixed mountain + flood
PIESTEWA PEAK         Summit trail                     — heat + overexertion
WHITE TANK MTNS       Western metro edge               — trail + technical
```

### Behavioral clusters (inferred from nature codes)
```
CLUSTER                       NATURE CODES                  PEAK WINDOW
──────────────────────────────────────────────────────────────────────────
Recreational — Underequipped  mountain rescue, heat          09:00–14:00 wknd
Flash Flood Stranded          check flooding, water rescue   14:00–20:00 Jul–Sep
```

*Full 7-cluster model pending richer FOIA data from MCSO + AZ DEMA.*

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
pixi run phoenix     # fetch Phoenix Fire 2019-2025
pixi run geocode     # geocode addresses
pixi run stats       # 10 analysis plots
pixi run map         # interactive map
pixi run build       # assemble site
pixi run serve       # preview at localhost:8080
```

---

## PROJECT STRUCTURE

```
ridgeline/
├── pixi.toml                         deps + tasks
├── .github/workflows/deploy.yml      weekly CI/CD → GitHub Pages
│
├── src/
│   ├── ingest/
│   │   ├── fetch_phoenix_fire.py     Phoenix Fire open data 2019-2025
│   │   ├── geocode_phoenix.py        Maricopa County geocoder
│   │   ├── fetch_gis.py              Maricopa + Pima GIS layers
│   │   └── clean_incidents.py        normalizer (future FOIA data)
│   ├── analysis/
│   │   ├── sar_stats.py              10 behavioral cluster plots
│   │   ├── wui_model.py              RF + logistic rescue prediction
│   │   └── weather_pull.py           Open-Meteo historical join
│   └── viz/
│       ├── build_map.py              interactive folium map
│       └── build_site.py             GitHub Pages assembler
│
├── data/
│   ├── external/                     GIS layers (tracked in git)
│   ├── raw/                          source CSVs (gitignored)
│   └── processed/                    parquets (gitignored)
│
└── site/                             built site (CI only)
```

---

## PIPELINE

```
pixi run phoenix     Phoenix Fire 2019-2025 → data/raw/
pixi run geocode     geocode → data/processed/
pixi run gis         GIS layers → data/external/
pixi run pipeline    phoenix + geocode

pixi run stats       10 plots → docs/
pixi run model       rescue prediction model
pixi run weather     Open-Meteo historical join

pixi run map         folium map → site/map.html
pixi run build       assemble site/
pixi run serve       localhost:8080
pixi run deploy      full pipeline + build + push
```

---

## ROADMAP

```
[x] Phoenix Fire real data — 86k incidents 2019-2025
[x] Geocoding — 82k addresses resolved
[x] Interactive map — Camelback, South Mtn, McDowell
[x] Maricopa GIS layers — parks, trails, trailheads
[x] 10 analysis plots on real data
[x] Rescue prediction model
[x] GitHub Pages CI/CD
[ ] Weather correlation — heat index × rescue volume
[ ] Full 7-cluster model — pending FOIA data
[ ] MCSO SAR incident log — FOIA in progress
[ ] AZ DEMA statewide mission log — FOIA in progress
[ ] Los Angeles — LAFD FOIA in progress
[ ] Portland, OR — open data
[ ] Seattle / King County — Sheriff SAR FOIA
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
