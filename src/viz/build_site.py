"""
ridgeline / src / viz / build_site.py

Reads processed data + analysis plots, renders the full
GitHub Pages site into site/.

Run:
    pixi run build
    python src/viz/build_site.py
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader
from rich.console import Console

ROOT     = Path(__file__).resolve().parents[2]
PROC_DIR = ROOT / "data" / "processed"
DOCS_DIR = ROOT / "docs"
SITE_DIR = ROOT / "site"
TMPL_DIR = ROOT / "src" / "viz" / "templates"

SITE_DIR.mkdir(parents=True, exist_ok=True)
TMPL_DIR.mkdir(parents=True, exist_ok=True)

console = Console()


def load_summary() -> dict:
    """Pull key stats from the cleaned dataset."""
    csv = PROC_DIR / "sar_incidents_clean.csv"
    if not csv.exists():
        return {}

    df = pd.read_csv(csv, parse_dates=["date"], low_memory=False)
    df = df[df["year"].between(2015, 2024)]

    summary = {
        "total_incidents":  len(df),
        "year_range":       f"{df['year'].min()}–{df['year'].max()}",
        "counties":         sorted(df["county"].dropna().unique().tolist()),
        "top_locations":    (
            df.groupby("location_name")
              .size()
              .sort_values(ascending=False)
              .head(8)
              .reset_index()
              .rename(columns={0: "count"})
              .to_dict(orient="records")
        ),
        "by_type": (
            df.groupby("incident_type")
              .size()
              .sort_values(ascending=False)
              .head(8)
              .reset_index()
              .rename(columns={0: "count"})
              .to_dict(orient="records")
        ),
        "annual": (
            df.groupby("year")
              .size()
              .reset_index(name="count")
              .to_dict(orient="records")
        ),
        "monthly": (
            df.groupby("month")
              .size()
              .reset_index(name="count")
              .to_dict(orient="records")
        ),
        "pct_heat":     round(df.get("is_heat_incident", pd.Series(dtype=bool)).mean() * 100, 1),
        "pct_weekend":  round(df.get("is_weekend", pd.Series(dtype=bool)).mean() * 100, 1),
    }
    return summary


def copy_assets() -> None:
    """Copy analysis plots into site/assets/."""
    assets = SITE_DIR / "assets"
    assets.mkdir(exist_ok=True)

    plots = list(DOCS_DIR.glob("*.png")) if DOCS_DIR.exists() else []
    for p in plots:
        shutil.copy2(p, assets / p.name)
        console.print(f"  [dim]copied {p.name}[/dim]")

    # Copy processed CSV for download
    csv = PROC_DIR / "sar_incidents_clean.csv"
    if csv.exists():
        shutil.copy2(csv, assets / csv.name)


def write_index(summary: dict) -> None:
    """Write the main index.html with embedded dashboard."""
    plots = [p.name for p in (SITE_DIR / "assets").glob("plot_*.png")] if (SITE_DIR / "assets").exists() else []
    plots.sort()

    summary_json = json.dumps(summary, default=str)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RIDGELINE — WUI SAR Analysis</title>
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow+Condensed:wght@700;900&family=Barlow:wght@300;400&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {{
    --sand:#c8a96e;--rust:#c0522a;--ember:#e8793a;--sage:#7a9e7e;
    --sky:#4a8fa8;--dark:#0d0f0c;--mid:#1a1e18;--surface:#242820;
    --border:#2e3429;--text:#d4cbb8;--muted:#6b6a5e;--alert:#d94f1e;
  }}
  * {{ box-sizing:border-box;margin:0;padding:0; }}
  body {{ background:var(--dark);color:var(--text);font-family:'Barlow',sans-serif;font-weight:300;min-height:100vh; }}
  .container {{ max-width:1200px;margin:0 auto;padding:32px 24px; }}
  .mono {{ font-family:'Share Tech Mono',monospace; }}
  .cond {{ font-family:'Barlow Condensed',sans-serif; }}
  header {{ border-bottom:1px solid var(--border);padding-bottom:24px;margin-bottom:32px; }}
  .sys-label {{ font-family:'Share Tech Mono',monospace;font-size:10px;letter-spacing:3px;color:var(--ember);margin-bottom:8px; }}
  h1 {{ font-family:'Barlow Condensed',sans-serif;font-weight:900;font-size:56px;line-height:0.9;text-transform:uppercase;color:#fff; }}
  h1 span {{ color:var(--ember); }}
  .subtitle {{ color:var(--muted);font-size:13px;margin-top:10px; }}
  .kpi-row {{ display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:32px; }}
  .kpi {{ background:var(--surface);border:1px solid var(--border);padding:20px; }}
  .kpi-label {{ font-family:'Share Tech Mono',monospace;font-size:9px;letter-spacing:2px;color:var(--muted);text-transform:uppercase;margin-bottom:8px; }}
  .kpi-value {{ font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:40px;color:var(--ember); }}
  .kpi-sub {{ font-size:11px;color:var(--muted);margin-top:4px; }}
  .section {{ margin-bottom:32px; }}
  .section-title {{ font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:13px;letter-spacing:2px;text-transform:uppercase;color:var(--sand);margin-bottom:16px;padding-bottom:8px;border-bottom:1px solid var(--border); }}
  .plot-grid {{ display:grid;grid-template-columns:1fr 1fr;gap:16px; }}
  .plot-card {{ background:var(--surface);border:1px solid var(--border);padding:4px; }}
  .plot-card img {{ width:100%;display:block; }}
  .chart-panel {{ background:var(--surface);border:1px solid var(--border);padding:20px;margin-bottom:16px; }}
  canvas {{ width:100%!important; }}
  footer {{ border-top:1px solid var(--border);padding-top:16px;margin-top:32px;font-family:'Share Tech Mono',monospace;font-size:9px;color:var(--muted);line-height:2; }}
  a {{ color:var(--sky); }}
  @media(max-width:700px){{ .kpi-row{{grid-template-columns:1fr 1fr}}.plot-grid{{grid-template-columns:1fr}} }}
</style>
</head>
<body>
<div class="container">
  <header>
    <div class="sys-label">▶ RIDGELINE // Wildland-Urban Interface SAR Analysis</div>
    <h1>Ridge<span>line</span></h1>
    <div class="subtitle">Search &amp; Rescue call volume at the urban–wilderness edge · Phoenix · Tucson · Arizona</div>
    <div style="margin-top:12px;">
      <a href="map.html" style="font-family:monospace;font-size:11px;letter-spacing:2px;
         color:var(--ember);text-decoration:none;border:1px solid var(--ember);
         padding:5px 12px;text-transform:uppercase;">▶ OPEN INTERACTIVE MAP</a>
    </div>
  </header>

  <div class="kpi-row">
    <div class="kpi">
      <div class="kpi-label">Total Incidents</div>
      <div class="kpi-value">{summary.get('total_incidents', 0):,}</div>
      <div class="kpi-sub">{summary.get('year_range','')}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">AZ State / Year</div>
      <div class="kpi-value" style="color:var(--alert)">~600</div>
      <div class="kpi-sub">All 15 counties · DEMA</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Heat-Related</div>
      <div class="kpi-value" style="color:var(--sand)">{summary.get('pct_heat','—')}%</div>
      <div class="kpi-sub">of all incidents</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Weekend Calls</div>
      <div class="kpi-value" style="color:var(--sage)">{summary.get('pct_weekend','—')}%</div>
      <div class="kpi-sub">recreational pattern</div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">Annual Trend — Interactive</div>
    <div class="chart-panel">
      <canvas id="annualChart" height="120"></canvas>
    </div>
  </div>

  {"<div class='section'><div class='section-title'>Analysis — Behavioral Cluster Plots</div><div class='plot-grid'>" + "".join(f"<div class='plot-card'><img src='assets/{p}' alt='{p}' loading='lazy'></div>" for p in plots) + "</div></div>" if plots else "<!-- Plots will appear after running pixi run stats -->"}

  <div class="section">
    <div class="section-title">Data</div>
    <p style="font-size:13px;color:var(--muted);line-height:1.8;">
      Raw sources: NSAR incident database · Arizona DEMA SAR logs · Phoenix Fire Dept annual reports.<br>
      Processed dataset: <a href="assets/sar_incidents_clean.csv">Download CSV</a> ·
      Pipeline: <code style="color:var(--ember)">pixi run pipeline</code>
    </p>
  </div>

  <footer>
    RIDGELINE · WUI SAR Analysis · Arizona &nbsp;|&nbsp;
    Sources: AZ DEMA (~600 SAR/yr statewide) · Phoenix Fire Dept (200+ mtn rescues/yr) ·
    SARA Tucson (100+ missions/yr) · MCSO Aviation + Volunteers &nbsp;|&nbsp;
    Built with Python · pixi · GitHub Pages
  </footer>
</div>

<script>
const summary = {summary_json};
Chart.defaults.color = '#6b6a5e';
Chart.defaults.borderColor = '#2e3429';
Chart.defaults.font.family = "'Share Tech Mono', monospace";
Chart.defaults.font.size = 10;

if (summary.annual && summary.annual.length) {{
  new Chart(document.getElementById('annualChart'), {{
    type: 'bar',
    data: {{
      labels: summary.annual.map(d => d.year),
      datasets: [{{
        label: 'SAR Incidents',
        data: summary.annual.map(d => d.count),
        backgroundColor: '#e8793a',
        borderWidth: 0,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{ backgroundColor: '#1a1e18', borderColor: '#2e3429', borderWidth: 1 }}
      }},
      scales: {{
        x: {{ grid: {{ display: false }} }},
        y: {{ grid: {{ color: 'rgba(46,52,41,0.5)' }}, min: 0 }}
      }}
    }}
  }});
}}
</script>
</body>
</html>"""

    out = SITE_DIR / "index.html"
    out.write_text(html, encoding="utf-8")
    console.print(f"[green]✓[/green] index.html ({out.stat().st_size:,} bytes)")


def main() -> None:
    console.rule("[bold]RIDGELINE — Site Build[/bold]")
    summary = load_summary()
    copy_assets()
    write_index(summary)
    console.print(f"\n[green]✓[/green] Site ready → {SITE_DIR}/")
    console.print("[dim]  Preview: pixi run serve[/dim]")


if __name__ == "__main__":
    main()
