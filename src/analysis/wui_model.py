"""
ridgeline / src / analysis / wui_model.py

WUI incident severity & rescue-probability model.
Uses logistic regression (statsmodels) + a random forest (scikit-learn)
to predict whether an incident results in a full rescue vs self-rescue/false alarm.

Features:
  - hour, month, is_weekend
  - behavioral_cluster (one-hot)
  - edge_type (one-hot)
  - distance_from_trailhead_m
  - subjects_total

Outputs to docs/:
  - plot_model_feature_importance.png
  - plot_model_roc.png
  - model_coefficients.csv

Run:
    pixi run model
    python src/analysis/wui_model.py
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table
from sklearn.ensemble import RandomForestClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score, roc_curve
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
import statsmodels.api as sm

ROOT     = Path(__file__).resolve().parents[2]
PROC_DIR = ROOT / "data" / "processed"
DOCS_DIR = ROOT / "docs"
DOCS_DIR.mkdir(parents=True, exist_ok=True)

console = Console()

BG      = "#0d0f0c"
SURFACE = "#1a1e18"
BORDER  = "#2e3429"
TEXT    = "#d4cbb8"
MUTED   = "#6b6a5e"
EMBER   = "#e8793a"
SAND    = "#c8a96e"
SAGE    = "#7a9e7e"
SKY     = "#4a8fa8"
ALERT   = "#d94f1e"


def apply_theme(fig, axes) -> None:
    fig.patch.set_facecolor(BG)
    for ax in np.array(axes).flat:
        ax.set_facecolor(SURFACE)
        ax.tick_params(colors=MUTED, labelsize=8.5)
        ax.xaxis.label.set_color(SAND)
        ax.yaxis.label.set_color(SAND)
        for sp in ax.spines.values():
            sp.set_edgecolor(BORDER)
        ax.grid(color=BORDER, linewidth=0.4)


def load_data() -> pd.DataFrame:
    parquet = PROC_DIR / "sar_incidents_clean.parquet"
    csv     = PROC_DIR / "sar_incidents_clean.csv"
    if parquet.exists():
        df = pd.read_parquet(parquet)
    elif csv.exists():
        df = pd.read_csv(csv, low_memory=False)
    else:
        raise FileNotFoundError("Run `pixi run pipeline` first.")

    df["date"]  = pd.to_datetime(df["date"], errors="coerce")
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["hour"]  = pd.to_numeric(df.get("hour", pd.Series(dtype=float)), errors="coerce")
    df["is_weekend"] = df["date"].dt.dayofweek >= 5
    return df[df["year"].between(2015, 2024)].copy()


def build_model_df(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    Build feature matrix X and binary target y.
    Target: 1 = full rescue required, 0 = self-rescue / false alarm / refused transport
    """
    rescue_outcomes = {"Rescued / Evacuated", "Fatality"}
    df = df.copy()
    df["target"] = df["outcome"].isin(rescue_outcomes).astype(int)

    # Numeric features
    num_cols = ["hour", "month", "is_weekend", "subjects_total",
                "distance_from_trailhead_m", "duration_hours"]
    for c in num_cols:
        if c not in df.columns:
            df[c] = np.nan

    # One-hot: behavioral_cluster, edge_type
    cat_cols = ["behavioral_cluster", "edge_type"]
    dummies = []
    for c in cat_cols:
        if c in df.columns:
            d = pd.get_dummies(df[c].fillna("unknown"), prefix=c, drop_first=True)
            dummies.append(d)

    X = pd.concat(
        [df[num_cols].fillna(df[num_cols].median())] + dummies,
        axis=1
    ).astype(float)
    y = df["target"]

    # Drop rows with missing target
    mask = y.notna()
    return X[mask], y[mask]


def run_logistic(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    """Logistic regression with statsmodels for interpretable coefficients."""
    X_sm = sm.add_constant(X)
    try:
        model = sm.Logit(y, X_sm).fit(disp=False, maxiter=200)
        coef_df = pd.DataFrame({
            "feature":  X_sm.columns,
            "coef":     model.params.values,
            "pvalue":   model.pvalues.values,
            "odds_ratio": np.exp(model.params.values),
        }).sort_values("coef", key=abs, ascending=False)

        coef_df.to_csv(DOCS_DIR / "model_coefficients.csv", index=False)
        console.print("  [green]✓[/green] model_coefficients.csv")
        return coef_df
    except Exception as exc:
        console.print(f"  [yellow]⚠[/yellow] Logistic regression failed: {exc}")
        return pd.DataFrame()


def run_rf_importance(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    """Random forest + permutation importance."""
    rf = RandomForestClassifier(
        n_estimators=300, max_depth=8, random_state=42,
        class_weight="balanced", n_jobs=-1,
    )
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(rf, X, y, cv=cv, scoring="roc_auc")
    console.print(
        f"  RF cross-val ROC-AUC: "
        f"[cyan]{scores.mean():.3f}[/cyan] ± {scores.std():.3f}"
    )

    rf.fit(X, y)
    imp = pd.DataFrame({
        "feature":   X.columns,
        "importance": rf.feature_importances_,
    }).sort_values("importance", ascending=False).head(15)

    return rf, imp, scores


def plot_feature_importance(imp: pd.DataFrame, cv_scores: np.ndarray) -> None:
    fig, ax = plt.subplots(figsize=(11, 6))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(SURFACE)

    imp_plot = imp.sort_values("importance")
    colors   = [EMBER if imp_plot["importance"].max() == v else SAND
                for v in imp_plot["importance"]]
    ax.barh(imp_plot["feature"], imp_plot["importance"],
            color=colors, linewidth=0, height=0.68)

    ax.set_xlabel("Feature Importance (Mean Decrease Impurity)", color=SAND)
    ax.tick_params(colors=MUTED, labelsize=9)
    ax.set_title(
        f"What Predicts a Full Rescue? — Random Forest Feature Importance\n"
        f"5-fold CV ROC-AUC: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}",
        color="white", fontsize=12, fontweight="bold", pad=10
    )
    for sp in ax.spines.values():
        sp.set_edgecolor(BORDER)
    ax.grid(color=BORDER, linewidth=0.4, axis="x")
    ax.grid(visible=False, axis="y")
    fig.text(0.985, 0.01, "RIDGELINE project", color=BORDER,
             fontsize=7.5, ha="right", va="bottom", fontfamily="monospace")

    fig.tight_layout()
    path = DOCS_DIR / "plot_model_feature_importance.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    console.print("  [green]✓[/green] plot_model_feature_importance.png")


def plot_roc(rf, X: pd.DataFrame, y: pd.Series) -> None:
    from sklearn.model_selection import StratifiedKFold
    cv    = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    fig, ax = plt.subplots(figsize=(7, 6))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(SURFACE)

    mean_fpr  = np.linspace(0, 1, 100)
    tprs, aucs = [], []

    for fold, (train_idx, test_idx) in enumerate(cv.split(X, y)):
        rf_fold = RandomForestClassifier(
            n_estimators=200, max_depth=8,
            random_state=42, class_weight="balanced", n_jobs=-1)
        rf_fold.fit(X.iloc[train_idx], y.iloc[train_idx])
        proba = rf_fold.predict_proba(X.iloc[test_idx])[:, 1]
        fpr, tpr, _ = roc_curve(y.iloc[test_idx], proba)
        auc = roc_auc_score(y.iloc[test_idx], proba)
        aucs.append(auc)
        interp_tpr = np.interp(mean_fpr, fpr, tpr)
        interp_tpr[0] = 0.0
        tprs.append(interp_tpr)
        ax.plot(fpr, tpr, color=SAND, alpha=0.25, linewidth=1,
                label=f"Fold {fold+1} (AUC={auc:.2f})" if fold == 0 else "_")

    mean_tpr = np.mean(tprs, axis=0)
    mean_tpr[-1] = 1.0
    ax.plot(mean_fpr, mean_tpr, color=EMBER, linewidth=2.5,
            label=f"Mean ROC (AUC = {np.mean(aucs):.3f})")
    ax.plot([0,1],[0,1], linestyle="--", color=MUTED, linewidth=1)

    ax.set_xlabel("False Positive Rate", color=SAND)
    ax.set_ylabel("True Positive Rate", color=SAND)
    ax.set_title("ROC Curve — Rescue Prediction Model\n5-fold Cross Validation",
                 color="white", fontsize=12, fontweight="bold")
    ax.legend(facecolor=SURFACE, edgecolor=BORDER, labelcolor=TEXT, fontsize=9)
    ax.tick_params(colors=MUTED, labelsize=9)
    for sp in ax.spines.values():
        sp.set_edgecolor(BORDER)
    ax.grid(color=BORDER, linewidth=0.4)
    fig.text(0.985, 0.01, "RIDGELINE project", color=BORDER,
             fontsize=7.5, ha="right", fontfamily="monospace")

    fig.tight_layout()
    path = DOCS_DIR / "plot_model_roc.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    console.print("  [green]✓[/green] plot_model_roc.png")


def main() -> None:
    console.rule("[bold]RIDGELINE — WUI Rescue Prediction Model[/bold]")
    df = load_data()
    X, y = build_model_df(df)
    console.print(f"  Features: [cyan]{X.shape[1]}[/cyan]  ·  "
                  f"Samples: [cyan]{len(X):,}[/cyan]  ·  "
                  f"Rescue rate: [cyan]{y.mean():.1%}[/cyan]")

    coef_df       = run_logistic(X, y)
    rf, imp, scores = run_rf_importance(X, y)
    plot_feature_importance(imp, scores)
    plot_roc(rf, X, y)
    console.rule("[green]Model done[/green]")


if __name__ == "__main__":
    main()
