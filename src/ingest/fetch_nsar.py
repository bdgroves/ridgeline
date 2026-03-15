"""
ridgeline / src / ingest / fetch_nsar.py

Fetches SAR incident data from publicly available sources:
  1. NSAR (National SAR) database export (public CSV)
  2. Arizona DEMA incident log (where available)
  3. Synthetic seed data for development when upstream is unavailable

Run:
    pixi run ingest
    python src/ingest/fetch_nsar.py --source nsar
    python src/ingest/fetch_nsar.py --source seed   # offline dev mode
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

import httpx
import pandas as pd
import typer
import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

console = Console()
app = typer.Typer(help="Ridgeline — SAR data ingestion")

# ---------------------------------------------------------------------------
# Source registry
# ---------------------------------------------------------------------------
SOURCES: dict[str, dict] = {
    "nsar": {
        "name": "NSAR Incident Database",
        "url": "https://data.nsar.org/incidents/export.csv",  # public endpoint
        "file": "nsar_incidents_raw.csv",
        "description": "National SAR incident database — all US missions",
    },
    "az_dema": {
        "name": "Arizona DEMA SAR Log",
        "url": "https://dema.az.gov/sites/default/files/SAR_incidents.csv",
        "file": "az_dema_raw.csv",
        "description": "Arizona Dept of Emergency & Military Affairs incident log",
    },
    "phoenix_fire": {
        "name": "Phoenix Fire Mountain Rescue Stats",
        "url": None,  # scraped / manual — see notes
        "file": "phoenix_fire_mountain_rescue.csv",
        "description": "City of Phoenix Fire Dept annual mountain rescue counts",
        "manual": True,
    },
}


# ---------------------------------------------------------------------------
# Seed / synthetic data (offline dev mode)
# Full behavioral cluster model — each cluster has its own time-of-day,
# day-of-week, seasonal, location, and incident-type signature.
# ---------------------------------------------------------------------------

# ── Locations ──────────────────────────────────────────────────────────────
# (name, county, lat, lon, terrain_type, edge_type)
# edge_type: "deep" = interior wilderness  |  "soft" = walk-out-your-door accessible
LOCATIONS = [
    # Phoenix / Maricopa — named trailheads
    ("Camelback Mtn - Echo Canyon",     "Maricopa", 33.5194, -111.9749, "mountain",     "named_trailhead"),
    ("Camelback Mtn - Cholla Trail",    "Maricopa", 33.5244, -111.9603, "mountain",     "named_trailhead"),
    ("South Mountain - Holbert Trail",  "Maricopa", 33.3476, -112.0540, "mountain",     "named_trailhead"),
    ("South Mountain - National Trail", "Maricopa", 33.3420, -111.9950, "mountain",     "named_trailhead"),
    ("Piestewa Peak",                   "Maricopa", 33.5307, -112.0197, "mountain",     "named_trailhead"),
    ("McDowell Sonoran Preserve",       "Maricopa", 33.6918, -111.7951, "desert",       "named_trailhead"),
    ("White Tank Mountain",             "Maricopa", 33.5971, -112.5476, "mountain",     "named_trailhead"),
    ("Superstition Wilderness",         "Maricopa", 33.4559, -111.3732, "backcountry",  "deep"),
    # Phoenix / Maricopa — soft WUI edge (no trailhead, just... the desert)
    ("South Mountain - Residential Edge, Ahwatukee",  "Maricopa", 33.3350, -112.0100, "desert_edge",  "soft"),
    ("McDowell - Scottsdale Neighborhood Edge",        "Maricopa", 33.6700, -111.8100, "desert_edge",  "soft"),
    ("North Phoenix Desert Washes (I-17 corridor)",   "Maricopa", 33.7100, -112.1000, "wash",         "soft"),
    ("Cave Creek Wash Corridor",                      "Maricopa", 33.8300, -112.0000, "wash",         "soft"),
    ("Peoria / Glendale Desert Edge",                 "Maricopa", 33.5800, -112.2500, "desert_edge",  "soft"),
    ("Tempe/Chandler Canal Corridor",                 "Maricopa", 33.4100, -111.9000, "wash",         "soft"),
    ("Gilbert / Queen Creek Wash",                    "Maricopa", 33.3500, -111.7500, "wash",         "soft"),
    # Tucson / Pima — named trailheads
    ("Sabino Canyon",                   "Pima",     32.3535, -110.7867, "canyon",       "named_trailhead"),
    ("Tanque Verde Falls",              "Pima",     32.2632, -110.6865, "canyon",       "named_trailhead"),
    ("Bear Canyon / Seven Falls",       "Pima",     32.3703, -110.7776, "canyon",       "named_trailhead"),
    ("Mount Lemmon / Catalinas",        "Pima",     32.4432, -110.7883, "mountain",     "named_trailhead"),
    ("Rincon Mountains",                "Pima",     32.1784, -110.5913, "backcountry",  "deep"),
    ("Catalina State Park",             "Pima",     32.4231, -110.9094, "desert",       "named_trailhead"),
    ("Tucson Mountain Park",            "Pima",     32.2498, -111.1614, "desert",       "named_trailhead"),
    # Tucson / Pima — soft WUI edge
    ("Catalina Foothills Neighborhood Edge",  "Pima", 32.3300, -110.9200, "desert_edge", "soft"),
    ("Tanque Verde / Pantano Wash Corridor",  "Pima", 32.2300, -110.7500, "wash",        "soft"),
    ("Rincon Valley Suburban Edge",           "Pima", 32.1500, -110.7000, "desert_edge", "soft"),
    ("Sam Hughes / University Neighborhood Washes", "Pima", 32.2300, -110.9400, "wash", "soft"),
]

# ── Behavioral cluster definitions ─────────────────────────────────────────
# Each cluster defines the statistical distribution of its incidents.
# hour_weights: list of 24 values (index = hour 0–23)
# dow_weights:  list of 7 values (Mon=0 … Sun=6)
# month_weights: list of 12 values (Jan=0 … Dec=11)
# preferred_edge: "soft" | "named_trailhead" | "deep" | "wash" | "any"
# incident_types: [(label, weight), ...]
# outcomes: [(label, weight), ...]
# caller_contexts: [(label, weight), ...]
# activity_at_onsets: [(label, weight), ...]
# distance_from_trailhead_m: (mean, std)  — negative = not near any trailhead
# subjects_range: (min, max)
# n_per_year: approximate annual incident count for this cluster

def _flat(n): return [1/n] * n

CLUSTERS = {

    "dog_walker": {
        "label": "Dog Walker",
        "description": "Owner follows dog off-trail into wash or preserve; loses bearing at dusk/dawn",
        "n_per_year": 55,
        "hour_weights":  [0,0,0,0,0,1,4,8,5,2,1,1,1,1,1,1,3,6,7,4,2,1,0,0],
        "dow_weights":   [5,5,5,5,5,4,4],   # slight weekday lean (before/after work)
        "month_weights": [3,3,4,5,5,4,3,3,4,5,5,4],  # year-round, dip in peak summer
        "preferred_edge": "soft",
        "incident_types": [
            ("Lost / Disoriented",  0.55),
            ("Traumatic Injury",    0.15),  # trip chasing dog
            ("Heat Exhaustion",     0.15),
            ("Dehydration",         0.10),
            ("Other",               0.05),
        ],
        "outcomes": [
            ("Self-rescue",          0.35),
            ("Rescued / Evacuated",  0.55),
            ("Refused Transport",    0.08),
            ("Fatality",             0.02),
        ],
        "caller_contexts": [
            ("Self — cell phone",    0.50),
            ("Neighbor / witness",   0.25),
            ("Did not call / found", 0.15),
            ("Friend / family",      0.10),
        ],
        "activity_at_onsets": [
            ("Walking dog off-leash",   0.60),
            ("Walking dog on-leash",    0.25),
            ("Chasing escaped dog",     0.15),
        ],
        "distance_from_trailhead_m": (800, 600),   # usually not at a trailhead
        "subjects_range": (1, 2),
        "duration_mean_h": 1.8,
    },

    "casual_proximity": {
        "label": "Casual Proximity",
        "description": "Errand runner, commuter, or resident who drifts into desert from adjacent street/parking lot",
        "n_per_year": 70,
        "hour_weights":  [0,0,0,0,0,1,2,3,4,5,5,4,3,4,5,4,5,6,5,3,2,1,0,0],
        "dow_weights":   [4,4,4,4,4,5,6],   # weekend slightly higher (more casual foot traffic)
        "month_weights": [3,3,4,5,6,5,4,4,5,5,4,3],
        "preferred_edge": "soft",
        "incident_types": [
            ("Lost / Disoriented",  0.50),
            ("Heat Exhaustion",     0.25),
            ("Traumatic Injury",    0.10),
            ("Dehydration",         0.10),
            ("Other",               0.05),
        ],
        "outcomes": [
            ("Self-rescue",          0.40),
            ("Rescued / Evacuated",  0.50),
            ("Refused Transport",    0.08),
            ("Fatality",             0.02),
        ],
        "caller_contexts": [
            ("Self — cell phone",    0.60),
            ("Neighbor / witness",   0.20),
            ("Friend / family",      0.15),
            ("Did not call / found", 0.05),
        ],
        "activity_at_onsets": [
            ("Walking — errand / destination nearby",  0.35),
            ("Shortcut through preserve/wash",         0.30),
            ("Exercise walk from home",                0.20),
            ("Exploring — no plan",                    0.15),
        ],
        "distance_from_trailhead_m": (1200, 900),
        "subjects_range": (1, 3),
        "duration_mean_h": 1.5,
    },

    "party_spillover": {
        "label": "Party / Social Spillover",
        "description": "Someone leaves a house party, bar, or outdoor event near a preserve at night",
        "n_per_year": 45,
        "hour_weights":  [2,3,3,2,1,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3,5,7,8],
        "dow_weights":   [1,1,1,2,3,7,8],   # heavy Fri/Sat night
        "month_weights": [2,2,3,4,6,8,8,7,6,5,3,2],  # summer outdoor party season
        "preferred_edge": "soft",
        "incident_types": [
            ("Lost / Disoriented",  0.45),
            ("Traumatic Injury",    0.20),  # falls in dark
            ("Medical Emergency",   0.18),  # intoxication
            ("Heat Exhaustion",     0.10),
            ("Other",               0.07),
        ],
        "outcomes": [
            ("Rescued / Evacuated",  0.60),
            ("Self-rescue",          0.20),
            ("Refused Transport",    0.15),
            ("Fatality",             0.05),
        ],
        "caller_contexts": [
            ("Friend / family",      0.45),
            ("Self — cell phone",    0.30),
            ("Neighbor / witness",   0.20),
            ("Did not call / found", 0.05),
        ],
        "activity_at_onsets": [
            ("Left party / gathering on foot",  0.50),
            ("Bar / event near preserve",       0.25),
            ("Argument — walked off",           0.15),
            ("Night hike — impaired",           0.10),
        ],
        "distance_from_trailhead_m": (600, 500),
        "subjects_range": (1, 4),
        "duration_mean_h": 2.5,
    },

    "youth_incident": {
        "label": "Youth / Teen Incident",
        "description": "Runaway, teens using desert as party space, kids on bikes in washes",
        "n_per_year": 60,
        "hour_weights":  [1,1,1,0,0,0,0,1,1,2,2,2,2,2,3,4,5,5,4,4,3,3,2,2],
        "dow_weights":   [3,3,3,3,4,6,7],  # weekend spike; after school weekdays
        "month_weights": [2,2,3,4,5,7,8,8,5,4,3,2],  # summer break dominates
        "preferred_edge": "wash",
        "incident_types": [
            ("Lost / Disoriented",  0.35),
            ("Traumatic Injury",    0.25),  # bike crash, fall
            ("Heat Exhaustion",     0.20),
            ("Overdue Subject",     0.12),
            ("Other",               0.08),
        ],
        "outcomes": [
            ("Rescued / Evacuated",  0.55),
            ("Self-rescue",          0.25),
            ("Refused Transport",    0.15),
            ("Fatality",             0.05),
        ],
        "caller_contexts": [
            ("Parent / guardian",    0.40),
            ("Friend / family",      0.25),
            ("Self — cell phone",    0.20),
            ("Did not call / found", 0.10),
            ("Neighbor / witness",   0.05),
        ],
        "activity_at_onsets": [
            ("Biking in wash / canal",         0.30),
            ("Hangout — desert party spot",    0.25),
            ("Runaway from home",              0.20),
            ("Exploring on foot",              0.15),
            ("Dare / challenge",               0.10),
        ],
        "distance_from_trailhead_m": (1500, 1000),
        "subjects_range": (1, 6),
        "duration_mean_h": 2.0,
    },

    "homeless_medical": {
        "label": "Unhoused / Encampment",
        "description": "Medical emergency or welfare check in wash corridor or preserve interior",
        "n_per_year": 50,
        "hour_weights":  [2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,2,2,2],  # all hours
        "dow_weights":   _flat(7),
        "month_weights": [3,3,3,3,4,7,8,8,6,4,3,3],  # heat season dangerous
        "preferred_edge": "wash",
        "incident_types": [
            ("Medical Emergency",   0.40),
            ("Heat Stroke",         0.25),
            ("Heat Exhaustion",     0.15),
            ("Traumatic Injury",    0.10),
            ("Overdue Subject",     0.05),
            ("Other",               0.05),
        ],
        "outcomes": [
            ("Rescued / Evacuated",  0.65),
            ("Fatality",             0.15),
            ("Refused Transport",    0.15),
            ("False Alarm",          0.05),
        ],
        "caller_contexts": [
            ("Patrol found",         0.35),
            ("Neighbor / witness",   0.30),
            ("Friend / family",      0.20),
            ("Self — cell phone",    0.15),
        ],
        "activity_at_onsets": [
            ("Encampment — medical", 0.50),
            ("Wandering",            0.30),
            ("Unknown",              0.20),
        ],
        "distance_from_trailhead_m": (2000, 1200),
        "subjects_range": (1, 3),
        "duration_mean_h": 1.2,
    },

    "recreational_underequipped": {
        "label": "Recreational — Underequipped",
        "description": "Intentional hiker/runner who started unprepared: no water, wrong shoes, no map",
        "n_per_year": 180,
        "hour_weights":  [0,0,0,0,0,0,1,3,7,9,9,8,6,4,3,2,2,2,2,1,0,0,0,0],
        "dow_weights":   [3,3,3,3,4,7,8],
        "month_weights": [4,4,5,7,8,10,10,9,7,6,5,4],
        "preferred_edge": "named_trailhead",
        "incident_types": [
            ("Heat Exhaustion",     0.35),
            ("Heat Stroke",         0.12),
            ("Lost / Disoriented",  0.20),
            ("Traumatic Injury",    0.15),
            ("Dehydration",         0.10),
            ("Flash Flood Stranded",0.05),
            ("Other",               0.03),
        ],
        "outcomes": [
            ("Rescued / Evacuated",  0.68),
            ("Self-rescue",          0.15),
            ("Fatality",             0.07),
            ("Refused Transport",    0.07),
            ("False Alarm",          0.03),
        ],
        "caller_contexts": [
            ("Self — cell phone",    0.55),
            ("Friend / family",      0.20),
            ("Neighbor / witness",   0.15),
            ("Did not call / found", 0.10),
        ],
        "activity_at_onsets": [
            ("Hiking — no water",       0.30),
            ("Hiking — wrong footwear", 0.20),
            ("Running — over-extended", 0.20),
            ("Hiking — no map/plan",    0.20),
            ("Biking — mechanical failure then lost", 0.10),
        ],
        "distance_from_trailhead_m": (200, 300),
        "subjects_range": (1, 5),
        "duration_mean_h": 2.8,
    },

    "flash_flood_stranded": {
        "label": "Flash Flood Stranded",
        "description": "Group or individual caught in a wash or canyon by sudden monsoon flooding",
        "n_per_year": 40,
        "hour_weights":  [0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3,5,7,7,5,3,2,1,0],
        "dow_weights":   _flat(7),
        "month_weights": [0,0,0,0,0,1,5,5,4,1,0,0],  # monsoon only (Jul–Sep)
        "preferred_edge": "wash",
        "incident_types": [
            ("Flash Flood Stranded", 0.75),
            ("Traumatic Injury",     0.15),
            ("Medical Emergency",    0.07),
            ("Fatality",             0.03),
        ],
        "outcomes": [
            ("Rescued / Evacuated",  0.75),
            ("Self-rescue",          0.10),
            ("Fatality",             0.10),
            ("False Alarm",          0.05),
        ],
        "caller_contexts": [
            ("Self — cell phone",    0.55),
            ("Neighbor / witness",   0.25),
            ("Friend / family",      0.15),
            ("Did not call / found", 0.05),
        ],
        "activity_at_onsets": [
            ("Swimming / tubing in canyon",  0.35),
            ("Hiking — crossed wash",        0.30),
            ("Driving — flooded road",       0.20),
            ("Camping — wash campsite",      0.15),
        ],
        "distance_from_trailhead_m": (400, 800),
        "subjects_range": (1, 15),
        "duration_mean_h": 3.5,
    },
}

# Resources available per county / severity
RESOURCES_MARICOPA = [
    "Phoenix Fire — Ground Team",
    "Helicopter (MCSO Fox-1)",
    "Helicopter (MCSO Fox-2)",
    "Helicopter (DPS Ranger)",
    "MCSO SAR — Ground",
    "Technical Rescue Team (TRT)",
    "K9 Unit (Mark9)",
    "ATV / UTV Team",
    "Mounted Unit",
    "Multi-Agency",
    "Scott's Dale Fire",
]
RESOURCES_PIMA = [
    "SARA Volunteers",
    "PCSD SAR Unit",
    "Rural Metro Fire",
    "Helicopter (DPS Ranger)",
    "Pima Co. SAR Divers",
    "SAMSAR (Mounted)",
    "Southwest Rescue Dogs (K9)",
    "Multi-Agency",
    "Tucson Fire",
]


def build_seed_data() -> pd.DataFrame:
    """
    Generate realistic synthetic SAR incident records for the Phoenix/Tucson
    WUI corridor using the full 7-cluster behavioral model.

    Each cluster has its own time-of-day, day-of-week, seasonal, location,
    incident-type, caller-context, and activity-at-onset distributions.
    """
    import random
    import numpy as np

    rng    = random.Random(42)
    np_rng = np.random.default_rng(42)

    # Index locations by edge_type for fast cluster matching
    loc_by_edge: dict[str, list] = {}
    for loc in LOCATIONS:
        et = loc[5]
        loc_by_edge.setdefault(et, []).append(loc)
    loc_by_edge["any"] = list(LOCATIONS)
    # wash cluster also OK with soft edge
    loc_by_edge["wash"] = loc_by_edge.get("wash", []) + loc_by_edge.get("soft", [])

    records = []
    years   = list(range(2015, 2025))

    for cluster_key, cfg in CLUSTERS.items():
        n_total = cfg["n_per_year"] * len(years)

        # Pick the location pool for this cluster
        edge = cfg["preferred_edge"]
        loc_pool = loc_by_edge.get(edge, loc_by_edge["any"])

        inc_types, inc_w   = zip(*cfg["incident_types"])
        outcomes,  out_w   = zip(*cfg["outcomes"])
        callers,   cal_w   = zip(*cfg["caller_contexts"])
        activities,act_w   = zip(*cfg["activity_at_onsets"])

        dist_mean, dist_std = cfg["distance_from_trailhead_m"]

        for _ in range(n_total):
            year  = rng.choice(years)
            month = rng.choices(range(1, 13), weights=cfg["month_weights"])[0]
            day   = rng.randint(1, 28)
            dow   = rng.choices(range(7), weights=cfg["dow_weights"])[0]
            hour  = rng.choices(range(24), weights=cfg["hour_weights"])[0]
            minute = rng.randint(0, 59)

            inc_dt = f"{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:00"

            loc = rng.choice(loc_pool)
            loc_name, county, lat, lon, terrain, edge_type = loc

            # Lat/lon jitter
            lat_j = lat + np_rng.normal(0, 0.004)
            lon_j = lon + np_rng.normal(0, 0.004)

            inc_type = rng.choices(inc_types, weights=inc_w)[0]
            outcome  = rng.choices(outcomes,  weights=out_w)[0]
            caller   = rng.choices(callers,   weights=cal_w)[0]
            activity = rng.choices(activities, weights=act_w)[0]

            # Time-of-day bucket
            if   hour in range(5, 9):   tod = "dawn"
            elif hour in range(9, 17):  tod = "day"
            elif hour in range(17, 21): tod = "dusk"
            else:                       tod = "night"

            # Duration
            duration_h = max(0.3, round(
                np_rng.normal(cfg["duration_mean_h"], cfg["duration_mean_h"] * 0.4), 1
            ))

            # Distance from trailhead
            dist_m = max(0, int(np_rng.normal(dist_mean, dist_std)))

            # Subjects
            s_min, s_max = cfg["subjects_range"]
            subjects = rng.randint(s_min, s_max)

            # Resources
            pool = RESOURCES_MARICOPA if county == "Maricopa" else RESOURCES_PIMA
            n_resp = rng.randint(1, 3)
            resp   = rng.sample(pool, min(n_resp, len(pool)))

            records.append({
                "incident_id":              f"AZ-{year}{month:02d}{day:02d}-{rng.randint(1000,9999)}",
                "datetime":                 inc_dt,
                "date":                     f"{year}-{month:02d}-{day:02d}",
                "year":                     year,
                "month":                    month,
                "hour":                     hour,
                "day_of_week_num":          dow,
                "location_name":            loc_name,
                "county":                   county,
                "state":                    "AZ",
                "latitude":                 round(lat_j, 6),
                "longitude":               round(lon_j, 6),
                "terrain_type":             terrain,
                "edge_type":               edge_type,
                "behavioral_cluster":       cluster_key,
                "cluster_label":           cfg["label"],
                "incident_type":           inc_type,
                "outcome":                  outcome,
                "caller_context":          caller,
                "activity_at_onset":       activity,
                "time_of_day_bucket":      tod,
                "distance_from_trailhead_m": dist_m,
                "resources":               "; ".join(resp),
                "duration_hours":          duration_h,
                "subjects_total":          subjects,
                "data_source":             "seed",
            })

    df = pd.DataFrame(records)
    df["date"]     = pd.to_datetime(df["date"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("date").reset_index(drop=True)

    console.print(f"[dim]Seed data: {len(df):,} records across {len(CLUSTERS)} behavioral clusters[/dim]")
    return df


# ---------------------------------------------------------------------------
# Phoenix Fire manual/curated data
# ---------------------------------------------------------------------------

PHOENIX_FIRE_ANNUAL = {
    # year: total_mountain_rescues (from Phoenix Fire Dept press releases)
    2009: 130, 2010: 135, 2011: 138, 2012: 140,
    2013: 150, 2014: 160, 2015: 165, 2016: 175,
    2017: 185, 2018: 210, 2019: 215, 2020: 195,
    2021: 230, 2022: 220, 2023: 200, 2024: 159,
}

CAMELBACK_ANNUAL = {
    # Camelback alone (Phoenix Fire + media reports)
    2009: 48, 2010: 50, 2011: 50, 2012: 52,
    2013: 55, 2014: 60, 2015: 63, 2016: 68,
    2017: 75, 2018: 90, 2019: 92, 2020: 82,
    2021: 95, 2022: 88, 2023: 78, 2024: 60,
}


def save_phoenix_fire() -> Path:
    """Serialize the curated Phoenix Fire annual stats to CSV."""
    rows = []
    for year in sorted(PHOENIX_FIRE_ANNUAL):
        rows.append({
            "year":                    year,
            "phoenix_mtn_rescues":     PHOENIX_FIRE_ANNUAL[year],
            "camelback_rescues":       CAMELBACK_ANNUAL.get(year),
            "piestewa_rescues":        None,   # TODO: scrape when available
            "south_mountain_rescues":  None,
            "data_source":             "phoenix_fire_curated",
            "notes":                   "Compiled from Phoenix Fire Dept press releases and media reports",
        })
    df = pd.DataFrame(rows)
    out = RAW_DIR / "phoenix_fire_mountain_rescue.csv"
    df.to_csv(out, index=False)
    console.print(f"[green]✓[/green] Phoenix Fire annual stats → {out.name}")
    return out


# ---------------------------------------------------------------------------
# HTTP fetch helper
# ---------------------------------------------------------------------------

def try_fetch(url: str, dest: Path, timeout: int = 30) -> bool:
    """Attempt to download a remote CSV. Returns True on success."""
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as prog:
            prog.add_task(f"Fetching {url} …", total=None)
            with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as r:
                r.raise_for_status()
                dest.write_bytes(b"".join(r.iter_bytes()))
        console.print(f"[green]✓[/green] Downloaded → {dest.name} ({dest.stat().st_size:,} bytes)")
        return True
    except Exception as exc:
        console.print(f"[yellow]⚠[/yellow]  Could not fetch {url}: {exc}")
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.command()
def main(
    source: Annotated[str, typer.Option("--source", "-s",
        help="Data source: nsar | az_dema | phoenix_fire | seed | all")] = "all",
    force: Annotated[bool, typer.Option("--force", "-f",
        help="Re-download even if file exists")] = False,
):
    """Fetch raw SAR incident data from public sources."""
    console.rule("[bold sand]RIDGELINE — Data Ingest[/bold sand]")

    fetched: list[Path] = []

    if source in ("phoenix_fire", "all"):
        fetched.append(save_phoenix_fire())

    if source in ("nsar", "all"):
        dest = RAW_DIR / SOURCES["nsar"]["file"]
        if not dest.exists() or force:
            ok = try_fetch(SOURCES["nsar"]["url"], dest)
            if not ok:
                console.print("[dim]Falling back to seed data for NSAR slot.[/dim]")
                source = "seed"  # fall through
        else:
            console.print(f"[dim]Skipping NSAR download (file exists, use --force to refresh)[/dim]")
            fetched.append(dest)

    if source in ("az_dema", "all"):
        dest = RAW_DIR / SOURCES["az_dema"]["file"]
        if not dest.exists() or force:
            ok = try_fetch(SOURCES["az_dema"]["url"], dest)
            if not ok:
                console.print("[dim]DEMA endpoint unavailable — continuing without.[/dim]")
        else:
            fetched.append(dest)

    if source in ("seed", "all"):
        dest = RAW_DIR / "seed_incidents.csv"
        if not dest.exists() or force:
            df = build_seed_data()
            df.to_csv(dest, index=False)
            console.print(f"[green]✓[/green] Seed data → {dest.name}")
        fetched.append(dest)

    # Manifest
    manifest = {
        "generated":   date.today().isoformat(),
        "files":       [str(p.relative_to(ROOT)) for p in fetched],
        "record_counts": {},
    }
    for p in fetched:
        try:
            manifest["record_counts"][p.name] = len(pd.read_csv(p))
        except Exception:
            manifest["record_counts"][p.name] = "n/a"

    manifest_path = RAW_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    console.print(f"\n[green]✓[/green] Manifest → {manifest_path.name}")

    # Summary table
    t = Table(title="Raw data files", show_header=True, header_style="bold")
    t.add_column("File",    style="cyan")
    t.add_column("Records", justify="right")
    for fname, count in manifest["record_counts"].items():
        t.add_row(fname, str(count))
    console.print(t)


if __name__ == "__main__":
    app()
