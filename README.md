# RIDGELINE

> SAR call volume analysis at the wildland-urban interface — Arizona  
> Pure Python · pixi · GitHub Pages

Phoenix and Tucson sit directly against some of the most technical desert terrain in North America. **The wilderness is right there** — and "right there" includes dog walkers following their dog into a wash at dusk, someone leaving a house party on the Catalina foothills edge at midnight, a kid biking down a canal corridor in July, and yes, someone who took a wrong turn walking to Starbucks.

RIDGELINE models the full behavioral spectrum of WUI incidents using public SAR data and a 7-cluster behavioral model with realistic time-of-day, day-of-week, and location signatures.

---

## Quick Start

```bash
git clone https://github.com/bdgroves/ridgeline.git
cd ridgeline

# Install pixi if needed
curl -fsSL https://pixi.sh/install.sh | bash

# Install all deps (Python only — no R)
pixi install

# Full pipeline
pixi run pipeline     # ingest + clean → parquet
pixi run stats        # 10 behavioral analysis plots → docs/
pixi run model        # WUI rescue prediction model
pixi run build        # assemble GitHub Pages site
pixi run serve        # preview at localhost:8080
```

---

## Project Structure

```
ridgeline/
├── pixi.toml                        # Python-only deps + all tasks
├── src/
│   ├── ingest/
│   │   ├── fetch_nsar.py            # NSAR/DEMA fetch; 7-cluster seed generator
│   │   └── clean_incidents.py       # normalize → parquet
│   ├── analysis/
│   │   ├── sar_stats.py             # 10 matplotlib/seaborn plots
│   │   ├── wui_model.py             # logistic + random forest rescue model
│   │   └── weather_pull.py          # Open-Meteo historical join
│   └── viz/
│       └── build_site.py            # GitHub Pages site assembler
├── data/
│   ├── raw/                         # downloaded CSVs (gitignored)
│   └── processed/                   # parquet + clean CSV
├── docs/                            # plot PNGs + summary CSVs
├── site/                            # built site (gitignored, CI deploys)
└── .github/workflows/deploy.yml     # weekly CI/CD
```

---

## Behavioral Clusters

| Cluster | Est./yr | Peak window | Location type | Who calls |
|---------|---------|-------------|---------------|-----------|
| Recreational — Underequipped | 180 | 9am–2pm weekends | Named trailhead | Self |
| Casual Proximity | 70 | All day | Soft WUI edge | Self / witness |
| Youth / Teen | 60 | 2pm–midnight weekends | Wash corridors | Parent |
| Dog Walker | 55 | 6–8am / 6–8pm weekdays | Soft edge & washes | Self / neighbor |
| Unhoused / Medical | 50 | All hours | Wash corridors | Patrol found |
| Party / Social Spillover | 45 | 10pm–2am Fri/Sat | Soft residential edge | Friend |
| Flash Flood Stranded | 40 | 2–8pm Jul–Sep | Wash / canyon | Self |

---

## Analysis Outputs (10 plots)

| Plot | What it shows |
|------|---------------|
| `plot_01_annual_volume` | Annual SAR incidents by county |
| `plot_02_monthly_pattern` | Seasonal distribution |
| `plot_03_incident_types` | Incident type breakdown |
| `plot_04_heatmap_tod_type` | **Time-of-day × incident type heatmap** |
| `plot_05_cluster_location` | **Behavioral cluster × location type** |
| `plot_06_dow_by_cluster` | Day-of-week signature per cluster |
| `plot_07_hourly_clusters` | Hour-of-day fingerprint per cluster |
| `plot_08_caller_context` | Who called 911? per cluster |
| `plot_09_activity_at_onset` | What were they doing? |
| `plot_10_distance_trailhead` | Distance from trailhead by cluster |

---

## Tasks

| Command | What it does |
|---------|-------------|
| `pixi run ingest` | Fetch NSAR/DEMA; generate seed data |
| `pixi run clean` | Normalize + validate → parquet |
| `pixi run pipeline` | ingest + clean |
| `pixi run stats` | 10 analysis plots → docs/ |
| `pixi run model` | Rescue prediction model (RF + logistic) |
| `pixi run weather` | Open-Meteo historical pull |
| `pixi run analyze` | stats + model |
| `pixi run build` | Assemble site/ |
| `pixi run deploy` | Full pipeline + analyze + build |
| `pixi run serve` | Preview at localhost:8080 |
| `pixi run notebook` | JupyterLab |

---

## Deployment

Enable GitHub Pages: **Settings → Pages → Source: GitHub Actions**  
CI runs on every push to `main` and weekly (Monday 06:00 UTC).

---

*Built with Python · pixi · matplotlib · seaborn · scikit-learn · statsmodels · Open-Meteo · GitHub Pages*
