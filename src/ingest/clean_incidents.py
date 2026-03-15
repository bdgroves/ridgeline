"""
ridgeline / src / ingest / clean_incidents.py

Reads all raw CSVs, normalizes column names, validates types,
and writes a single cleaned Parquet + CSV to data/processed/.

Run:
    pixi run clean
    python src/ingest/clean_incidents.py
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console
from rich.table import Table

ROOT      = Path(__file__).resolve().parents[2]
RAW_DIR   = ROOT / "data" / "raw"
PROC_DIR  = ROOT / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

console = Console()

# ---------------------------------------------------------------------------
# Column name normalization map
# Covers NSAR export headers, DEMA headers, and our seed schema
# ---------------------------------------------------------------------------
COL_MAP: dict[str, str] = {
    # NSAR export variants
    "mission_number":        "incident_id",
    "mission number":        "incident_id",
    "incident_number":       "incident_id",
    "start_date":            "date",
    "mission_date":          "date",
    "incident_date":         "date",
    "incident_state":        "state",
    "state_name":            "state",
    "county_name":           "county",
    "location":              "location_name",
    "incident_location":     "location_name",
    "subject_category":      "incident_type",
    "mission_type":          "incident_type",
    "outcome_category":      "outcome",
    "total_subjects":        "subjects_total",
    "number_of_subjects":    "subjects_total",
    "lat":                   "latitude",
    "lon":                   "longitude",
    "lng":                   "longitude",
    "long":                  "longitude",
    # Duration
    "hours":                 "duration_hours",
    "mission_hours":         "duration_hours",
    "total_hours":           "duration_hours",
}

REQUIRED_COLS = {
    "incident_id", "date", "state", "county",
    "location_name", "incident_type", "latitude", "longitude",
}

INCIDENT_TYPE_NORM: dict[str, str] = {
    # Raw strings → canonical labels
    r"heat.exhaus":    "Heat Exhaustion",
    r"heat.strok":     "Heat Stroke",
    r"heat":           "Heat (NOS)",
    r"lost|disori":    "Lost / Disoriented",
    r"trauma|injur|fall|broke": "Traumatic Injury",
    r"flood|strand|water": "Flash Flood Stranded",
    r"dehydrat":       "Dehydration",
    r"medical|cardiac|chest|seizure": "Medical Emergency",
    r"false.alarm|no.find":           "False Alarm",
    r"overdue":        "Overdue Subject",
}

TERRAIN_MAP: dict[str, str] = {
    r"camelback|piestewa|south.mountain|white.tank|mcdowell|superstition|lemmon|rincon|catalina.state|tucson.mountain": "mountain/desert",
    r"sabino|tanque.verde|bear.canyon|seven.falls|canyon": "canyon",
    r"wilderness|backcountry|remote": "backcountry",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase + strip columns, then apply COL_MAP."""
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    df = df.rename(columns={k: v for k, v in COL_MAP.items() if k in df.columns})
    return df


def normalize_incident_type(series: pd.Series) -> pd.Series:
    """Map free-text incident types to canonical labels."""
    def _map(val: str | None) -> str:
        if not isinstance(val, str):
            return "Unknown"
        v = val.lower()
        for pattern, label in INCIDENT_TYPE_NORM.items():
            if re.search(pattern, v):
                return label
        return val.title()
    return series.apply(_map)


def infer_terrain(location: pd.Series) -> pd.Series:
    """Guess terrain type from location name."""
    def _infer(name: str | None) -> str:
        if not isinstance(name, str):
            return "unknown"
        n = name.lower()
        for pattern, terrain in TERRAIN_MAP.items():
            if re.search(pattern, n):
                return terrain
        return "desert"
    return location.apply(_infer)


def validate(df: pd.DataFrame, source: str) -> tuple[pd.DataFrame, list[str]]:
    """Drop rows with critical missing fields; return (clean_df, warnings)."""
    warnings = []
    n_before = len(df)

    # Date parsing
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    n_bad_date = df["date"].isna().sum()
    if n_bad_date:
        warnings.append(f"{source}: {n_bad_date} rows with unparseable dates dropped")
    df = df.dropna(subset=["date"])

    # Arizona filter — only keep AZ incidents from multi-state sources
    if "state" in df.columns:
        df = df[df["state"].str.upper().isin(["AZ", "ARIZONA", "Arizona"])].copy()

    # Numeric coercion
    for col in ["latitude", "longitude", "duration_hours", "subjects_total"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Lat/lon sanity check (AZ bounding box roughly)
    if "latitude" in df.columns and "longitude" in df.columns:
        az_mask = (
            df["latitude"].between(31.0, 37.0) &
            df["longitude"].between(-115.0, -109.0)
        )
        n_oob = (~az_mask & df["latitude"].notna()).sum()
        if n_oob:
            warnings.append(f"{source}: {n_oob} rows outside AZ bounding box dropped")
        df = df[az_mask | df["latitude"].isna()].copy()

    n_dropped = n_before - len(df)
    if n_dropped:
        warnings.append(f"{source}: {n_dropped} total rows dropped during validation")

    return df, warnings


def add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Compute useful derived columns."""
    df["year"]          = df["date"].dt.year
    df["month"]         = df["date"].dt.month
    df["month_name"]    = df["date"].dt.strftime("%b")
    df["day_of_week"]   = df["date"].dt.day_name()
    df["is_weekend"]    = df["day_of_week"].isin(["Saturday", "Sunday"])
    df["season"]        = df["month"].map({
        12: "Winter", 1: "Winter", 2: "Winter",
        3: "Spring",  4: "Spring", 5: "Spring",
        6: "Summer",  7: "Summer", 8: "Summer",
        9: "Monsoon+Fall", 10: "Monsoon+Fall", 11: "Monsoon+Fall",
    })
    df["is_heat_incident"] = df["incident_type"].str.contains(
        "Heat", case=False, na=False
    )
    df["is_flood_incident"] = df["incident_type"].str.contains(
        "Flood", case=False, na=False
    )
    if "terrain_type" not in df.columns or df["terrain_type"].isna().all():
        df["terrain_type"] = infer_terrain(df.get("location_name", pd.Series()))
    return df


def load_raw_file(path: Path) -> pd.DataFrame | None:
    """Read a raw CSV, normalize columns, return None if unusable."""
    try:
        df = pd.read_csv(path, low_memory=False)
        if df.empty:
            console.print(f"[yellow]⚠[/yellow]  {path.name}: empty file, skipping")
            return None
        df = normalize_columns(df)
        df["data_source"] = path.stem
        return df
    except Exception as exc:
        console.print(f"[red]✗[/red]  {path.name}: {exc}")
        return None


def main() -> None:
    console.rule("[bold]RIDGELINE — Data Cleaning[/bold]")

    raw_files = sorted(RAW_DIR.glob("*.csv"))
    if not raw_files:
        console.print("[red]No raw CSV files found in data/raw/. Run `pixi run ingest` first.[/red]")
        return

    all_frames: list[pd.DataFrame] = []
    all_warnings: list[str] = []

    for path in raw_files:
        if path.name == "manifest.json":
            continue
        console.print(f"[cyan]→[/cyan] Processing {path.name} …")
        df = load_raw_file(path)
        if df is None:
            continue

        # Normalize incident types where column exists
        if "incident_type" in df.columns:
            df["incident_type"] = normalize_incident_type(df["incident_type"])
        else:
            df["incident_type"] = "Unknown"

        # Add minimal missing required cols
        for col in REQUIRED_COLS - set(df.columns):
            df[col] = None

        df, warns = validate(df, path.name)
        all_warnings.extend(warns)

        if not df.empty:
            all_frames.append(df)
            console.print(f"  [green]✓[/green] {len(df):,} valid records")

    if not all_frames:
        console.print("[red]No valid records after cleaning.[/red]")
        return

    combined = pd.concat(all_frames, ignore_index=True)
    combined = add_derived_fields(combined)

    # Deduplicate on incident_id (keep most recent source)
    if "incident_id" in combined.columns:
        n_before = len(combined)
        combined = combined.drop_duplicates(subset=["incident_id"], keep="last")
        n_deduped = n_before - len(combined)
        if n_deduped:
            console.print(f"[dim]Deduplicated {n_deduped} duplicate incident IDs[/dim]")

    combined = combined.sort_values("date").reset_index(drop=True)

    # ── Write outputs ──────────────────────────────────────────────────────
    csv_out     = PROC_DIR / "sar_incidents_clean.csv"
    parquet_out = PROC_DIR / "sar_incidents_clean.parquet"

    combined.to_csv(csv_out, index=False)
    pq.write_table(
        pa.Table.from_pandas(combined),
        parquet_out,
        compression="snappy",
    )

    console.print(f"\n[green]✓[/green] {csv_out.name}     ({csv_out.stat().st_size:,} bytes)")
    console.print(f"[green]✓[/green] {parquet_out.name} ({parquet_out.stat().st_size:,} bytes)")

    # ── Summary ────────────────────────────────────────────────────────────
    t = Table(title=f"Clean dataset — {len(combined):,} records", show_header=True)
    t.add_column("Metric")
    t.add_column("Value", justify="right")
    t.add_row("Date range",      f"{combined['date'].min().date()} → {combined['date'].max().date()}")
    t.add_row("Counties",         str(combined["county"].nunique()))
    t.add_row("Unique locations", str(combined["location_name"].nunique()))
    t.add_row("Incident types",   str(combined["incident_type"].nunique()))
    t.add_row("Heat incidents",   str(combined["is_heat_incident"].sum()))
    t.add_row("Flood incidents",  str(combined["is_flood_incident"].sum()))
    t.add_row("Weekend calls",    str(combined["is_weekend"].sum()))
    console.print(t)

    if all_warnings:
        console.print("\n[yellow]Warnings:[/yellow]")
        for w in all_warnings:
            console.print(f"  [yellow]⚠[/yellow]  {w}")


if __name__ == "__main__":
    main()
