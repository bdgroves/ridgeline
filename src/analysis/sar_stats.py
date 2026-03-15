"""
ridgeline / src / analysis / sar_stats.py

Full behavioral cluster analysis — pure Python/matplotlib/seaborn.
Ports all 10 plots from the R script, plus a cluster summary CSV.

Plots produced in docs/:
  01  annual volume by county
  02  monthly / seasonal pattern
  03  incident type distribution
  04  time-of-day × incident type heatmap      ← the money shot
  05  behavioral cluster × location type
  06  day-of-week signature per cluster
  07  hourly distribution per cluster
  08  caller context per cluster
  09  activity at onset
  10  distance from trailhead by cluster

Run:
    pixi run stats
    python src/analysis/sar_stats.py
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.patches import FancyBboxPatch
from rich.console import Console
from rich.table import Table

ROOT     = Path(__file__).resolve().parents[2]
PROC_DIR = ROOT / "data" / "processed"
DOCS_DIR = ROOT / "docs"
DOCS_DIR.mkdir(parents=True, exist_ok=True)

console = Console()

# ── Palette ────────────────────────────────────────────────────────────────
BG      = "#0d0f0c"
SURFACE = "#1a1e18"
BORDER  = "#2e3429"
TEXT    = "#d4cbb8"
MUTED   = "#6b6a5e"

EMBER = "#e8793a"
SAND  = "#c8a96e"
SAGE  = "#7a9e7e"
SKY   = "#4a8fa8"
ALERT = "#d94f1e"
RUST  = "#c0522a"

CLUSTER_COLORS: dict[str, str] = {
    "dog_walker":                   SAND,
    "casual_proximity":             SKY,
    "party_spillover":              ALERT,
    "youth_incident":               SAGE,
    "homeless_medical":             MUTED,
    "recreational_underequipped":   EMBER,
    "flash_flood_stranded":         "#5ba3c0",
}

CLUSTER_LABELS: dict[str, str] = {
    "dog_walker":                   "Dog Walker",
    "casual_proximity":             "Casual Proximity",
    "party_spillover":              "Party / Social Spillover",
    "youth_incident":               "Youth / Teen",
    "homeless_medical":             "Unhoused / Encampment",
    "recreational_underequipped":   "Recreational — Underequipped",
    "flash_flood_stranded":         "Flash Flood Stranded",
}

EDGE_COLORS: dict[str, str] = {
    "named_trailhead":  EMBER,
    "soft":             SAND,
    "wash":             SKY,
    "deep":             MUTED,
    "desert_edge":      RUST,
}

EDGE_LABELS: dict[str, str] = {
    "named_trailhead":  "Named Trailhead",
    "soft":             "Soft WUI Edge",
    "wash":             "Wash Corridor",
    "deep":             "Deep Backcountry",
    "desert_edge":      "Desert Edge",
}

SEASON_COLORS = {
    "Winter":       SKY,
    "Spring":       SAND,
    "Summer":       ALERT,
    "Monsoon/Fall": SAGE,
}

MONTH_ABBR = ["Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"]


# ── Theme helpers ──────────────────────────────────────────────────────────

def apply_theme(fig: plt.Figure, ax_or_axes) -> None:
    """Apply Ridgeline dark theme to a figure and its axes."""
    fig.patch.set_facecolor(BG)
    axes = ax_or_axes if hasattr(ax_or_axes, "__iter__") else [ax_or_axes]
    for ax in np.array(axes).flat:
        ax.set_facecolor(SURFACE)
        ax.tick_params(colors=MUTED, labelsize=8.5)
        ax.xaxis.label.set_color(SAND)
        ax.yaxis.label.set_color(SAND)
        for spine in ax.spines.values():
            spine.set_edgecolor(BORDER)
        ax.grid(color=BORDER, linewidth=0.4)


def styled_fig(w: float = 12, h: float = 6,
               title: str = "", subtitle: str = "",
               caption: str = "RIDGELINE project · seed data",
               nrows: int = 1, ncols: int = 1,
               **kwargs):
    """Create a dark-themed figure with title/subtitle/caption."""
    fig, axes = plt.subplots(nrows, ncols, figsize=(w, h), **kwargs)
    fig.patch.set_facecolor(BG)

    y_start = 0.97
    if title:
        fig.text(0.015, y_start, title, color="white", fontsize=13,
                 fontweight="bold", va="top", fontfamily="monospace")
        y_start -= 0.04
    if subtitle:
        fig.text(0.015, y_start, subtitle, color=MUTED, fontsize=8.5,
                 va="top", fontfamily="monospace")
    if caption:
        fig.text(0.985, 0.01, caption, color=BORDER, fontsize=7.5,
                 ha="right", va="bottom", fontfamily="monospace")

    return fig, axes


def save(fig: plt.Figure, name: str) -> None:
    path = DOCS_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    console.print(f"  [green]✓[/green] {name}")


# ── Data loading ───────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    parquet = PROC_DIR / "sar_incidents_clean.parquet"
    csv     = PROC_DIR / "sar_incidents_clean.csv"

    if parquet.exists():
        df = pd.read_parquet(parquet)
    elif csv.exists():
        df = pd.read_csv(csv, low_memory=False)
    else:
        raise FileNotFoundError(
            "No processed data found. Run `pixi run pipeline` first."
        )

    df["date"]  = pd.to_datetime(df["date"], errors="coerce")
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["hour"]  = pd.to_numeric(df.get("hour", pd.Series(dtype=float)),
                                errors="coerce")
    df["month_name"] = df["month"].apply(lambda m: MONTH_ABBR[int(m)-1]
                                          if pd.notna(m) else None)
    df["season"] = df["month"].map({
        12:"Winter", 1:"Winter",  2:"Winter",
        3:"Spring",  4:"Spring",  5:"Spring",
        6:"Summer",  7:"Summer",  8:"Summer",
        9:"Monsoon/Fall", 10:"Monsoon/Fall", 11:"Monsoon/Fall",
    })
    df["dow_name"] = df["date"].dt.day_name()
    df["is_weekend"] = df["date"].dt.dayofweek >= 5
    df["cluster_label"] = (
        df.get("behavioral_cluster", pd.Series(dtype=str))
          .map(CLUSTER_LABELS)
          .fillna(df.get("cluster_label", "Unknown"))
    )

    df = df[df["year"].between(2015, 2024)].copy()
    return df


# ── Plot 01 — Annual volume ────────────────────────────────────────────────

def plot_annual_volume(df: pd.DataFrame) -> None:
    annual = (df.groupby(["year","county"])
                .size().reset_index(name="n"))

    fig, ax = styled_fig(
        12, 5,
        title="Annual SAR Incidents — Maricopa & Pima Counties",
        subtitle="All behavioral clusters combined",
    )
    apply_theme(fig, ax)

    counties = annual["county"].unique()
    colors   = {"Maricopa": EMBER, "Pima": SKY}
    years    = sorted(annual["year"].unique())
    x        = np.arange(len(years))
    width    = 0.35

    bottoms = np.zeros(len(years))
    for county in ["Maricopa", "Pima"]:
        subset = annual[annual["county"] == county].set_index("year")
        vals   = [subset.loc[y, "n"] if y in subset.index else 0 for y in years]
        ax.bar(x, vals, width=0.7, bottom=bottoms,
               color=colors.get(county, MUTED), label=county, linewidth=0)
        bottoms += np.array(vals)

    ax.set_xticks(x)
    ax.set_xticklabels([str(y) for y in years], color=MUTED)
    ax.set_ylabel("Incidents", color=SAND)
    ax.legend(facecolor=SURFACE, edgecolor=BORDER, labelcolor=TEXT)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"{int(v):,}"))
    fig.tight_layout(rect=[0, 0.02, 1, 0.92])
    save(fig, "plot_01_annual_volume.png")


# ── Plot 02 — Monthly pattern ──────────────────────────────────────────────

def plot_monthly_pattern(df: pd.DataFrame) -> None:
    n_years = df["year"].nunique()
    monthly = (df.groupby(["month","month_name","season"])
                 .size().reset_index(name="n"))
    monthly["per_year"] = monthly["n"] / n_years
    monthly = monthly.sort_values("month")

    fig, ax = styled_fig(
        12, 5,
        title="Average Monthly SAR Incidents",
        subtitle="Heat season (Jun–Aug) dominates · Monsoon (Jul–Sep) adds canyon/wash flooding incidents",
    )
    apply_theme(fig, ax)

    for _, row in monthly.iterrows():
        ax.bar(row["month_name"], row["per_year"],
               color=SEASON_COLORS.get(row["season"], MUTED),
               linewidth=0, width=0.72)

    # Season legend
    from matplotlib.patches import Patch
    handles = [Patch(facecolor=c, label=s)
               for s, c in SEASON_COLORS.items()]
    ax.legend(handles=handles, facecolor=SURFACE,
              edgecolor=BORDER, labelcolor=TEXT)
    ax.set_ylabel("Avg Incidents / Year", color=SAND)
    ax.set_xlabel("")
    fig.tight_layout(rect=[0, 0.02, 1, 0.92])
    save(fig, "plot_02_monthly_pattern.png")


# ── Plot 03 — Incident type distribution ──────────────────────────────────

def plot_incident_types(df: pd.DataFrame) -> None:
    counts = (df["incident_type"].value_counts()
                .head(10).reset_index())
    counts.columns = ["incident_type", "n"]
    counts["pct"] = counts["n"] / counts["n"].sum()
    counts = counts.sort_values("n")

    palette = [c for c in [ALERT,RUST,EMBER,SAND,SAGE,SKY,"#5ba3c0","#7ab0c0","#aab8b0","#7a9e9e"]]

    fig, ax = styled_fig(
        12, 6,
        title="Incident Type Distribution",
        subtitle="Heat-related incidents dominate Phoenix · Flash flood and trauma dominate Tucson canyons",
    )
    apply_theme(fig, ax)

    bars = ax.barh(counts["incident_type"], counts["n"],
                   color=palette[:len(counts)], linewidth=0, height=0.68)
    for bar, pct in zip(bars, counts["pct"]):
        ax.text(bar.get_width() + counts["n"].max() * 0.01,
                bar.get_y() + bar.get_height() / 2,
                f"{pct:.0%}", va="center", color=SAND, fontsize=9)

    ax.set_xlabel("Total Incidents (2015–2024)", color=SAND)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"{int(v):,}"))
    ax.set_xlim(0, counts["n"].max() * 1.15)
    fig.tight_layout(rect=[0, 0.02, 1, 0.92])
    save(fig, "plot_03_incident_types.png")


# ── Plot 04 — Time-of-day × Incident type heatmap ─────────────────────────

def plot_heatmap_tod_type(df: pd.DataFrame) -> None:
    tod_order = ["dawn", "day", "dusk", "night"]
    tod_labels_map = {
        "dawn":  "Dawn\n5–8 am",
        "day":   "Day\n9 am–4 pm",
        "dusk":  "Dusk\n5–8 pm",
        "night": "Night\n9 pm–4 am",
    }

    top_types = df["incident_type"].value_counts().head(8).index.tolist()
    sub = df[df["incident_type"].isin(top_types)].copy()
    sub["tod"] = sub.get("time_of_day_bucket", pd.Series(dtype=str)).fillna("day")

    pivot = (sub.groupby(["incident_type","tod"])
               .size().unstack(fill_value=0)
               .reindex(columns=tod_order, fill_value=0))

    # Normalise row-wise (% of each incident type)
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0)

    # Order rows by total count descending
    row_order = df[df["incident_type"].isin(top_types)]["incident_type"]\
                  .value_counts().index.tolist()
    pivot_pct = pivot_pct.reindex(row_order)

    fig, ax = styled_fig(
        11, 6,
        title="When Do Incidents Happen? — Time of Day × Incident Type",
        subtitle=(
            "% of each incident type occurring in each window\n"
            "Party spillover & trauma peak at night  ·  Heat calls cluster midday  ·  Dog walkers at dawn/dusk"
        ),
    )
    apply_theme(fig, ax)

    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("rl", [SURFACE, ALERT])

    im = ax.imshow(pivot_pct.values, cmap=cmap, aspect="auto",
                   vmin=0, vmax=pivot_pct.values.max())

    # Cell labels
    for r in range(pivot_pct.shape[0]):
        for c in range(pivot_pct.shape[1]):
            val = pivot_pct.iloc[r, c]
            ax.text(c, r, f"{val:.0%}", ha="center", va="center",
                    color="white", fontsize=9, fontweight="bold")

    ax.set_xticks(range(len(tod_order)))
    ax.set_xticklabels([tod_labels_map[t] for t in tod_order],
                       color=TEXT, fontsize=9)
    ax.set_yticks(range(len(row_order)))
    ax.set_yticklabels(row_order, color=TEXT, fontsize=9)
    ax.tick_params(length=0)
    ax.set_xlabel("")

    # Colorbar
    cb = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cb.ax.yaxis.set_tick_params(color=MUTED, labelsize=8)
    cb.set_label("% of incident type", color=SAND, fontsize=9)
    cb.outline.set_edgecolor(BORDER)

    fig.tight_layout(rect=[0, 0.02, 1, 0.88])
    save(fig, "plot_04_heatmap_tod_type.png")


# ── Plot 05 — Behavioral cluster × location type ──────────────────────────

def plot_cluster_location(df: pd.DataFrame) -> None:
    if "edge_type" not in df.columns:
        console.print("[yellow]  ⚠ edge_type column missing — skipping plot 05[/yellow]")
        return

    sub = df[df["cluster_label"] != "Unknown"].copy()
    grp = (sub.groupby(["cluster_label","edge_type"])
              .size().reset_index(name="n"))
    totals = grp.groupby("cluster_label")["n"].transform("sum")
    grp["pct"] = grp["n"] / totals

    edge_order = ["named_trailhead","soft","desert_edge","wash","deep"]
    edge_order = [e for e in edge_order if e in grp["edge_type"].unique()]

    # Cluster order by total incidents
    cluster_order = (sub.groupby("cluster_label").size()
                       .sort_values(ascending=True).index.tolist())

    fig, ax = styled_fig(
        13, 6,
        title="Where Do Incidents Happen? — Behavioral Cluster × Location Type",
        subtitle=(
            "Dog walkers & casual proximity: soft WUI edge and wash corridors — rarely at a trailhead\n"
            "Recreational underequipped: overwhelmingly at named trailheads  ·  "
            "Party spillover: soft residential edge"
        ),
    )
    apply_theme(fig, ax)

    lefts = np.zeros(len(cluster_order))
    for edge in edge_order:
        vals = []
        for cl in cluster_order:
            row = grp[(grp["cluster_label"] == cl) & (grp["edge_type"] == edge)]
            vals.append(float(row["pct"].iloc[0]) if len(row) else 0.0)
        ax.barh(cluster_order, vals, left=lefts,
                color=EDGE_COLORS.get(edge, MUTED),
                label=EDGE_LABELS.get(edge, edge),
                linewidth=0, height=0.65)
        lefts += np.array(vals)

    ax.axvline(0.5, color=MUTED, linewidth=0.6, linestyle="--")
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
    ax.set_xlabel("% of Cluster's Incidents", color=SAND)
    ax.legend(facecolor=SURFACE, edgecolor=BORDER, labelcolor=TEXT,
              loc="lower right", fontsize=9)
    ax.tick_params(axis="y", labelsize=9.5)
    fig.tight_layout(rect=[0, 0.02, 1, 0.88])
    save(fig, "plot_05_cluster_location.png")


# ── Plot 06 — Day-of-week signature per cluster ────────────────────────────

def plot_dow_by_cluster(df: pd.DataFrame) -> None:
    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    dow_abbr  = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

    sub = df[df["cluster_label"] != "Unknown"].copy()
    sub["dow_name"] = pd.Categorical(sub["dow_name"], categories=dow_order)
    clusters = sorted(sub["cluster_label"].unique())
    n_cl = len(clusters)
    ncols = 4
    nrows = int(np.ceil(n_cl / ncols))

    fig, axes = styled_fig(
        15, 7,
        title="Day-of-Week Signature — Each Behavioral Cluster",
        subtitle="Dashed = flat 14.3%/day  ·  Party spillover spikes Fri/Sat  ·  Dog walkers uniform  ·  Youth peaks weekend",
        nrows=nrows, ncols=ncols,
    )
    apply_theme(fig, axes)

    flat = 1 / 7

    for i, cl in enumerate(clusters):
        ax = axes.flat[i]
        sub_cl = sub[sub["cluster_label"] == cl]
        counts = sub_cl["dow_name"].value_counts().reindex(dow_order, fill_value=0)
        pcts   = counts / counts.sum()
        color  = next((v for k,v in CLUSTER_COLORS.items()
                       if CLUSTER_LABELS.get(k) == cl), EMBER)
        ax.bar(dow_abbr, pcts.values, color=color, linewidth=0, width=0.72)
        ax.axhline(flat, color=MUTED, linewidth=0.7, linestyle="--")
        ax.set_title(cl, color=SAND, fontsize=8.5, pad=4)
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=0))
        ax.tick_params(axis="x", labelsize=7.5)

    # Hide unused axes
    for j in range(i + 1, nrows * ncols):
        axes.flat[j].set_visible(False)

    fig.tight_layout(rect=[0, 0.02, 1, 0.91])
    save(fig, "plot_06_dow_by_cluster.png")


# ── Plot 07 — Hourly distribution per cluster ──────────────────────────────

def plot_hourly_clusters(df: pd.DataFrame) -> None:
    if df["hour"].isna().all():
        console.print("[yellow]  ⚠ hour column empty — skipping plot 07[/yellow]")
        return

    sub = df[df["cluster_label"] != "Unknown"].dropna(subset=["hour"]).copy()
    sub["hour"] = sub["hour"].astype(int)
    clusters = sorted(sub["cluster_label"].unique())
    n_cl  = len(clusters)
    ncols = 4
    nrows = int(np.ceil(n_cl / ncols))

    fig, axes = styled_fig(
        15, 7,
        title="Hour-of-Day Call Distribution — Behavioral Clusters",
        subtitle=(
            "Each cluster has a distinct temporal fingerprint\n"
            "Party spillover: late night  ·  Dog walkers: dawn/dusk peaks  ·  "
            "Recreational: midmorning  ·  Unhoused: flat all day"
        ),
        nrows=nrows, ncols=ncols,
    )
    apply_theme(fig, axes)

    hour_ticks = [0, 6, 12, 18, 23]
    hour_labels = ["12am","6am","Noon","6pm","11pm"]

    for i, cl in enumerate(clusters):
        ax = axes.flat[i]
        sub_cl = sub[sub["cluster_label"] == cl]
        counts = sub_cl["hour"].value_counts().reindex(range(24), fill_value=0)
        pcts   = counts / counts.sum()
        color  = next((v for k,v in CLUSTER_COLORS.items()
                       if CLUSTER_LABELS.get(k) == cl), EMBER)

        ax.bar(range(24), pcts.values, color=color, linewidth=0, width=0.85)
        for vx in [6, 12, 18]:
            ax.axvline(vx, color=BORDER, linewidth=0.5, linestyle=":")
        ax.set_title(cl, color=SAND, fontsize=8.5, pad=4)
        ax.set_xticks(hour_ticks)
        ax.set_xticklabels(hour_labels, fontsize=7)
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1, decimals=0))

    for j in range(i + 1, nrows * ncols):
        axes.flat[j].set_visible(False)

    fig.tight_layout(rect=[0, 0.02, 1, 0.89])
    save(fig, "plot_07_hourly_clusters.png")


# ── Plot 08 — Caller context per cluster ──────────────────────────────────

def plot_caller_context(df: pd.DataFrame) -> None:
    if "caller_context" not in df.columns:
        return

    sub = df[df["cluster_label"] != "Unknown"].dropna(subset=["caller_context"])
    grp = (sub.groupby(["cluster_label","caller_context"])
              .size().reset_index(name="n"))
    totals = grp.groupby("cluster_label")["n"].transform("sum")
    grp["pct"] = grp["n"] / totals

    caller_colors = {
        "Self — cell phone":    EMBER,
        "Friend / family":      SAND,
        "Parent / guardian":    RUST,
        "Neighbor / witness":   SAGE,
        "Patrol found":         SKY,
        "Did not call / found": MUTED,
    }

    callers = list(caller_colors.keys())
    callers = [c for c in callers if c in grp["caller_context"].unique()]

    cluster_order = (sub.groupby("cluster_label").size()
                       .sort_values(ascending=True).index.tolist())

    fig, ax = styled_fig(
        13, 6,
        title="Who Made the 911 Call? — by Behavioral Cluster",
        subtitle=(
            "Unhoused & party spillover: most likely found by patrol or witness — not self-reported\n"
            "Dog walkers & hikers: predominantly self-report via cell phone"
        ),
    )
    apply_theme(fig, ax)

    lefts = np.zeros(len(cluster_order))
    for caller in callers:
        vals = []
        for cl in cluster_order:
            row = grp[(grp["cluster_label"] == cl) & (grp["caller_context"] == caller)]
            vals.append(float(row["pct"].iloc[0]) if len(row) else 0.0)
        ax.barh(cluster_order, vals, left=lefts,
                color=caller_colors[caller], label=caller,
                linewidth=0, height=0.65)
        lefts += np.array(vals)

    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
    ax.set_xlabel("% of Cluster's Incidents", color=SAND)
    ax.legend(facecolor=SURFACE, edgecolor=BORDER, labelcolor=TEXT,
              loc="lower right", fontsize=8.5)
    ax.tick_params(axis="y", labelsize=9.5)
    fig.tight_layout(rect=[0, 0.02, 1, 0.88])
    save(fig, "plot_08_caller_context.png")


# ── Plot 09 — Activity at onset ────────────────────────────────────────────

def plot_activity_at_onset(df: pd.DataFrame) -> None:
    if "activity_at_onset" not in df.columns:
        return

    counts = (df["activity_at_onset"].dropna()
                .value_counts().head(16).reset_index())
    counts.columns = ["activity", "n"]
    counts["pct"] = counts["n"] / counts["n"].sum()
    counts = counts.sort_values("n")

    fig, ax = styled_fig(
        13, 7,
        title="What Were They Doing? — Activity at Onset of Incident",
        subtitle=(
            "The full WUI picture: serious backcountry hikers · dog walkers · errand runners · "
            "party-goers · kids on bikes"
        ),
    )
    apply_theme(fig, ax)

    bars = ax.barh(counts["activity"], counts["n"],
                   color=EMBER, linewidth=0, height=0.7)
    for bar, row in zip(bars, counts.itertuples()):
        ax.text(bar.get_width() + counts["n"].max() * 0.008,
                bar.get_y() + bar.get_height() / 2,
                f"{row.n:,}  ({row.pct:.0%})",
                va="center", color=SAND, fontsize=8.5)

    ax.set_xlabel("Total Incidents (2015–2024)", color=SAND)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"{int(v):,}"))
    ax.set_xlim(0, counts["n"].max() * 1.2)
    fig.tight_layout(rect=[0, 0.02, 1, 0.92])
    save(fig, "plot_09_activity_at_onset.png")


# ── Plot 10 — Distance from trailhead by cluster ──────────────────────────

def plot_distance_trailhead(df: pd.DataFrame) -> None:
    if "distance_from_trailhead_m" not in df.columns:
        return

    sub = df[
        (df["cluster_label"] != "Unknown") &
        df["distance_from_trailhead_m"].notna() &
        (df["distance_from_trailhead_m"] >= 0)
    ].copy()
    sub["dist_km"] = sub["distance_from_trailhead_m"] / 1000

    # Order clusters by median distance ascending
    order = (sub.groupby("cluster_label")["dist_km"]
               .median().sort_values().index.tolist())

    fig, ax = styled_fig(
        12, 6,
        title="How Far from a Trailhead? — Distance at Incident by Cluster",
        subtitle=(
            "Dog walkers & casual proximity: incidents far from any official trailhead\n"
            "Recreational underequipped: starts at a trailhead — that's where it goes wrong"
        ),
    )
    apply_theme(fig, ax)

    for i, cl in enumerate(order):
        vals  = sub[sub["cluster_label"] == cl]["dist_km"].dropna()
        color = next((v for k,v in CLUSTER_COLORS.items()
                      if CLUSTER_LABELS.get(k) == cl), EMBER)
        bp = ax.boxplot(vals, positions=[i], vert=False, widths=0.55,
                        patch_artist=True, notch=False,
                        flierprops=dict(marker="o", markerfacecolor=MUTED,
                                        markersize=2.5, alpha=0.4, linewidth=0),
                        whiskerprops=dict(color=color, linewidth=1),
                        capprops=dict(color=color, linewidth=1.2),
                        medianprops=dict(color="white", linewidth=1.5),
                        boxprops=dict(facecolor=color + "88", edgecolor=color,
                                      linewidth=1))

    ax.set_yticks(range(len(order)))
    ax.set_yticklabels(order, fontsize=9.5)
    ax.set_xlabel("Distance from Nearest Trailhead (km)", color=SAND)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"{v:.1f} km"))
    fig.tight_layout(rect=[0, 0.02, 1, 0.90])
    save(fig, "plot_10_distance_trailhead.png")


# ── Cluster summary CSV ────────────────────────────────────────────────────

def write_summary_csv(df: pd.DataFrame) -> None:
    n_years = df["year"].nunique()

    rows = []
    for cl in df["cluster_label"].unique():
        if cl == "Unknown":
            continue
        sub = df[df["cluster_label"] == cl]
        top_type = sub["incident_type"].value_counts().idxmax() \
                   if not sub.empty else ""
        rows.append({
            "cluster":              cl,
            "total_incidents":      len(sub),
            "avg_per_year":         round(len(sub) / n_years, 1),
            "pct_weekend":          round(sub["is_weekend"].mean() * 100, 1),
            "pct_self_called":      round(
                sub["caller_context"].str.contains("Self", na=False).mean() * 100, 1)
                if "caller_context" in sub else None,
            "median_duration_hrs":  round(sub["duration_hours"].median(), 1)
                if "duration_hours" in sub else None,
            "median_dist_trail_m":  round(sub["distance_from_trailhead_m"].median(), 0)
                if "distance_from_trailhead_m" in sub else None,
            "top_incident_type":    top_type,
        })

    out = pd.DataFrame(rows).sort_values("total_incidents", ascending=False)
    path = DOCS_DIR / "sar_cluster_summary.csv"
    out.to_csv(path, index=False)
    console.print(f"  [green]✓[/green] sar_cluster_summary.csv")

    # Rich table to terminal
    t = Table(title="Cluster Summary", show_header=True, header_style="bold")
    for col in out.columns:
        t.add_column(col, style="cyan" if col == "cluster" else "")
    for _, row in out.iterrows():
        t.add_row(*[str(v) for v in row])
    console.print(t)


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    console.rule("[bold]RIDGELINE — Behavioral Cluster Analysis[/bold]")

    df = load_data()
    n_years = df["year"].nunique()
    console.print(
        f"  [cyan]{len(df):,}[/cyan] records · "
        f"[cyan]{n_years}[/cyan] years · "
        f"[cyan]{df['cluster_label'].nunique()}[/cyan] clusters\n"
    )

    plot_annual_volume(df)
    plot_monthly_pattern(df)
    plot_incident_types(df)
    plot_heatmap_tod_type(df)        # ← the money shot
    plot_cluster_location(df)        # ← cluster × location
    plot_dow_by_cluster(df)
    plot_hourly_clusters(df)
    plot_caller_context(df)
    plot_activity_at_onset(df)
    plot_distance_trailhead(df)

    write_summary_csv(df)

    console.rule("[green]Done — 10 plots + summary CSV[/green]")


if __name__ == "__main__":
    main()
