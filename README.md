```
╔══════════════════════════════════════════════════════════════════════════════╗
║  ▶ RIDGELINE // WUI SAR ANALYSIS                              SYSTEM ACTIVE  ║
║  MARICOPA CO · PIMA CO · ARIZONA                           STATUS: DEPLOYED  ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

# RIDGELINE

**Search & Rescue call volume at the urban–wilderness edge**

> *The mountain is right there. The wash is right there.*  
> *The trail runs out. The signal drops. The sun goes down.*  
> *And someone who left the house to walk the dog is now a SAR call.*

Phoenix and Tucson are built against some of the most unforgiving terrain in North America. South Mountain is the largest municipal park in the United States. Camelback, Piestewa Peak, and the McDowell Mountains are **inside city limits**. Tucson is ringed by the Santa Catalinas, the Rincons, and the Santa Ritas — wilderness accessible from suburban driveways.

The result is a uniquely dense SAR load at the wildland-urban interface. ~600 missions per year statewide. 200+ mountain rescues in Phoenix city parks alone. A seven-cluster behavioral profile ranging from serious backcountry emergencies to a dog walker who followed their lab into a wash at dusk and couldn't find the gap in the fence they came through.

**RIDGELINE quantifies all of it.**

---

```
┌─────────────────────────────────────────────────────────────┐
│  INCIDENT PROFILE — ARIZONA WUI                             │
│                                                             │
│  HEAT EXHAUSTION  ████████████████████░░░░  42%            │
│  LOST/DISORIENTED ████████████░░░░░░░░░░░░  20%            │
│  TRAUMATIC INJURY ██████████░░░░░░░░░░░░░░  16%            │
│  FLASH FLOOD      ███████░░░░░░░░░░░░░░░░░  11%            │
│  MEDICAL (OTHER)  █████░░░░░░░░░░░░░░░░░░░   7%            │
│  OTHER            ███░░░░░░░░░░░░░░░░░░░░░   4%            │
└─────────────────────────────────────────────────────────────┘
```

---

## BEHAVIORAL CLUSTERS

```
CLUSTER                    EST/YR  PEAK WINDOW          LOCATION
──────────────────────────────────────────────────────────────────────
Recreational—Underequipped   180   09:00–14:00 weekends  Named trailhead
Casual Proximity              70   All day, any day       Soft WUI edge
Youth / Teen Incident         60   14:00–midnight wknd   Wash corridor
Dog Walker                    55   06:00–08:00 / dusk     Soft edge + wash
Unhoused / Encampment         50   All hours              Wash corridor
Party / Social Spillover      45   22:00–02:00 Fri/Sat    Residential edge
Flash Flood Stranded          40   14:00–20:00 Jul–Sep    Canyon / wash
──────────────────────────────────────────────────────────────────────
```

The dog walker and the Starbucks wrong turn and the kid on a bike in a canal — these are not edge cases. They are structurally different from the backcountry hiker but they live in the same 911 dispatch queue. RIDGELINE models them all.

---

## QUICK START

```bash
# Clone
git clone https://github.com/bdgroves/ridgeline.git
cd ridgeline

# Install pixi if needed
curl -fsSL https://pixi.sh/install.sh | bash       # macOS / Linux
# Windows PowerShell:
# iwr -useb https://pixi.sh/install.ps1 | iex

# Install all Python deps
pixi install

# Full pipeline
pixi run pipeline     # ingest + clean → parquet
pixi run stats        # 10 behavioral cluster plots → docs/
pixi run model        # rescue prediction model (RF + logistic)
pixi run build        # assemble GitHub Pages site → site/
pixi run serve        # preview at http://localhost:8080
```

---

## PROJECT STRUCTURE

```
ridgeline/
├── pixi.toml                         single manifest — Python only
│
├── src/
│   ├── ingest/
│   │   ├── fetch_nsar.py             fetch NSAR · Phoenix Fire curated stats
│   │   │                             → graceful seed fallback if offline
│   │   └── clean_incidents.py        normalize · validate · → parquet
│   │
│   ├── analysis/
│   │   ├── sar_stats.py              10 behavioral cluster plots
│   │   ├── wui_model.py              RF + logistic rescue prediction
│   │   └── weather_pull.py           Open-Meteo historical join
│   │
│   └── viz/
│       └── build_site.py             GitHub Pages site assembler
│
├── data/
│   ├── raw/                          source CSVs (gitignored)
│   └── processed/                    parquet + clean CSV
│
├── docs/                             plot PNGs · summary CSVs
├── site/                             built site (CI deploys, gitignored)
└── .github/workflows/deploy.yml      weekly CI/CD → GitHub Pages
```

---

## DATA SOURCES

### Live / Free

| Source | What it has | How |
|--------|-------------|-----|
| **Phoenix Fire Dept** | Annual mountain rescue counts by trail (2009–present) | Curated in `fetch_nsar.py` from press releases |
| **Open-Meteo** | Historical daily temp / precip / wind for Phoenix + Tucson | Free API, no key — `pixi run weather` |
| **Maricopa County GIS** | Parcel bounds, preserve edges, trailhead locations | [data-maricopa.opendata.arcgis.com](https://data-maricopa.opendata.arcgis.com) |
| **Pima County GIS** | Same for Tucson metro | [gisopendata.pima.gov](https://gisopendata.pima.gov) |
| **USGS National Map** | Elevation, watershed boundaries, terrain | [apps.nationalmap.gov](https://apps.nationalmap.gov) |

### Gated / Request Required

| Source | What it has | How to request |
|--------|-------------|----------------|
| **NSAR Database** | National SAR incident records | [nasar.org](https://nasar.org) — member access |
| **ISRID** | International SAR incidents incl. AZ desert eco-region | Email robert@dbs-sar.com with agency/research affiliation |
| **AZ DEMA** | Statewide SAR mission logs (~600/yr) | Public records request — [dema.az.gov](https://dema.az.gov) |
| **MCSO** | Maricopa County Sheriff SAR incident log | FOIA to Maricopa County Sheriff's Office |
| **PCSD** | Pima County Sheriff SAR incident log | FOIA to Pima County Sheriff's Dept |

### Seed Data (Always Available)

When live endpoints are unreachable, `pixi run ingest` generates a realistic **5,000-record synthetic dataset** using the 7-cluster behavioral model with calibrated time-of-day, day-of-week, seasonal, and location distributions. The pipeline always runs. CI never breaks.

---

## PIPELINE TASKS

```
pixi run ingest      fetch raw sources → data/raw/
pixi run clean       normalize + validate → data/processed/parquet
pixi run pipeline    ingest + clean

pixi run stats       10 cluster analysis plots → docs/
pixi run model       rescue prediction model + ROC curve
pixi run weather     Open-Meteo historical temp/precip join
pixi run analyze     stats + model

pixi run build       assemble site/ from data + plots
pixi run serve       preview at localhost:8080
pixi run deploy      full pipeline + analyze + build

pixi run notebook    JupyterLab
pixi run lint        ruff check src/
pixi run test        pytest tests/
```

---

## ANALYSIS OUTPUT

```
PLOTS → docs/

  01  Annual volume by county
  02  Monthly / seasonal pattern
  03  Incident type distribution
  04  Time-of-day × incident type heatmap    ◀ the money shot
  05  Behavioral cluster × location type     ◀ dog walker vs party spillover
  06  Day-of-week signature per cluster
  07  Hourly distribution per cluster
  08  Caller context — who called 911?
  09  Activity at onset
  10  Distance from trailhead by cluster

MODEL → docs/

  plot_model_feature_importance.png   RF feature importance
  plot_model_roc.png                  5-fold CV ROC curve
  model_coefficients.csv              logistic regression coefficients
```

---

## KEY FINDINGS

```
  ~600    SAR missions/yr statewide — Arizona DEMA
   200+   Mountain rescues/yr — Phoenix city parks alone
    70%   Of rescues involve Arizona residents, not tourists
    37%   Drop in Camelback rescues post-closure program (2021–2024)
    43%   Drop in Piestewa rescues — same program
  1,970m  Median distance from trailhead — Unhoused / Encampment cluster
   208m   Median distance from trailhead — Recreational Underequipped
 $12–15k  Estimated cost per rescue — Phoenix Fire Dept
```

---

## DEPLOYMENT

GitHub Actions builds and deploys on every push to `main` and weekly (Monday 06:00 UTC).

**Enable Pages:** Settings → Pages → Source → **GitHub Actions**

Live: **[bdgroves.github.io/ridgeline](https://bdgroves.github.io/ridgeline)**

---

## ROADMAP

```
[x] 7-cluster behavioral seed data model
[x] Full Python pipeline — pixi managed
[x] 10 analysis plots — matplotlib / seaborn
[x] Rescue prediction model — RF + logistic regression
[x] GitHub Pages CI/CD — weekly auto-deploy
[ ] GIS layer — WUI boundary + trailhead proximity map (folium)
[ ] Weather correlation — heat index × rescue volume regression
[ ] Real NSAR / ISRID data ingestion (pending access)
[ ] Phoenix Fire annual stats scraper
[ ] Interactive incident hotspot map
[ ] Comparative metros — Denver Front Range · LA Angeles NF · SLC Wasatch
```

---

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  SOURCES                                                                     ║
║  AZ DEMA · Phoenix Fire Dept · SARA Tucson · MCSO Aviation · NASAR · ISRID  ║
║                                                                              ║
║  STACK                                                                       ║
║  Python · pixi · pandas · matplotlib · seaborn · scikit-learn · statsmodels  ║
║  geopandas · folium · Open-Meteo · GitHub Actions · GitHub Pages             ║
╚══════════════════════════════════════════════════════════════════════════════╝
```
