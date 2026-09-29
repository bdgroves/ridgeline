"""
ridgeline / src / analysis / heat.py

Heat and trail closures in Phoenix.

Since 2021 Phoenix has closed the Echo Canyon and Cholla trails on Camelback
and the Piestewa Peak trails from 9 a.m. to 5 p.m. whenever the National
Weather Service has an (Excessive/Extreme) Heat Warning in effect. In October
2024 the city proposed adding South Mountain and starting at 8 a.m.
(Phoenix Parks and Recreation Board, "Phoenix Trails and Heat Safety",
24 Oct 2024).

This script asks what the dispatch data can say about that:

  * mountain-rescue calls per day by daily high temperature,
  * on warning days, closure trails vs trails that never closed,
    before the program (2019-2020) and after (2021 on),
  * on warning days after 2021, whether calls at the closure trails
    fall inside or outside the closed hours.

Inputs (fetched when the network allows, otherwise read from the committed copy):
  data/processed/phoenix_fire_sar_clean.parquet  -> data/external/phoenix_mountain_calls.csv
  Open-Meteo archive, Sky Harbor                  -> data/external/phoenix_daily_weather.csv
  IEM VTEC archive, zone AZZ543 Central Phoenix   -> data/external/phoenix_heat_warnings.csv
Output:
  data/external/heat_report.json

Uses all 1,619 mountain-rescue calls, located or not: the trail group comes
from the dispatch address text, so geocoding doesn't limit it.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
EXT = ROOT / "data" / "external"
PARQUET = ROOT / "data" / "processed" / "phoenix_fire_sar_clean.parquet"
CALLS = EXT / "phoenix_mountain_calls.csv"
WEATHER = EXT / "phoenix_daily_weather.csv"
WARNINGS = EXT / "phoenix_heat_warnings.csv"
OUT = EXT / "heat_report.json"

ZONE = "AZZ543"            # NWS Phoenix zone "Central Phoenix"
MST = timezone(timedelta(hours=-7))   # Arizona: no daylight saving
PROGRAM_START = date(2021, 5, 1)       # first closures, summer 2021
CLOSE_HOURS = range(9, 17)             # 9 a.m. to 5 p.m.

# Trail groups from the dispatch address. Only addresses we are sure of are
# assigned; anything else is "other".
GROUPS = [
    ("Camelback (Echo Canyon, Cholla)", "closure",
     r"\bMCDONALD\s+DR\b|ECHO\s+CANYON|CHOLLA\s+(LN|TR|TRL|TRAIL)|CAMELBACK\s+(MOUNTAIN|MTN)"
     r"|\b51XX\s+N\s+INVERGORDON"),   # the Cholla trailhead's address from 2022 on
    ("Piestewa Peak", "closure", r"(PIESTEWA|SQUAW)\s+PEAK"),
    ("South Mountain", "south",
     r"PIMA\s+CANYON|\b1\d{2}XX\s+S\s+CENTRAL|VALLEY\s+VIEW\s+DR|DESERT\s+FOOTHILLS|SOUTH\s+(MOUNTAIN|MTN)\s+(PARK|PRESERVE)|HOLBERT"),
]
_GROUPS = [(n, k, re.compile(p, re.I)) for n, k, p in GROUPS]


def group_of(address: str) -> tuple[str, str]:
    for name, kind, rx in _GROUPS:
        if rx.search(str(address or "")):
            return name, kind
    return "Other trails", "other"


# ── inputs ──────────────────────────────────────────────────────────────────

def load_calls() -> pd.DataFrame:
    if PARQUET.exists():
        df = pd.read_parquet(PARQUET)
        df = df[df["incident_type"].astype(str).str.strip().str.lower() == "mountain rescue"].copy()
        df["datetime"] = pd.to_datetime(df["datetime"])
        out = pd.DataFrame({"date": df["datetime"].dt.date.astype(str),
                            "hour": df["datetime"].dt.hour,
                            "address": df["location_name"].astype(str)})
        out = out.dropna().sort_values(["date", "hour"])
        out.to_csv(CALLS, index=False)
    out = pd.read_csv(CALLS)
    g = out["address"].map(group_of)
    out["group"], out["kind"] = g.str[0], g.str[1]
    out["date"] = pd.to_datetime(out["date"]).dt.date
    return out


def load_weather(end: date) -> pd.DataFrame:
    try:
        r = httpx.get("https://archive-api.open-meteo.com/v1/archive", timeout=120, params={
            "latitude": 33.4342, "longitude": -112.0116, "start_date": "2018-01-01",
            "end_date": end.isoformat(), "timezone": "America/Phoenix",
            "temperature_unit": "fahrenheit",
            "daily": "temperature_2m_max,temperature_2m_min,apparent_temperature_max,precipitation_sum"})
        r.raise_for_status()
        pd.DataFrame(r.json()["daily"]).to_csv(WEATHER, index=False)
    except Exception as e:  # sandbox or outage: use the committed copy
        print(f"  weather: using cached copy ({e!r})")
    w = pd.read_csv(WEATHER)
    w["date"] = pd.to_datetime(w["time"]).dt.date
    return w.dropna(subset=["temperature_2m_max"])


def load_warnings() -> pd.DataFrame:
    try:
        r = httpx.get("https://mesonet.agron.iastate.edu/json/vtec_events_byugc.php", timeout=120,
                      params={"ugc": ZONE, "sdate": "2018-01-01", "edate": date.today().isoformat()})
        r.raise_for_status()
        ev = r.json().get("events", [])
        rows = [{"eventid": e["eventid"], "phenomena": e["phenomena"], "issue": e["issue"],
                 "expire": e["expire"], "name": e.get("name")}
                for e in ev if e.get("phenomena") in ("EH", "XH") and e.get("significance") == "W"]
        pd.DataFrame(rows).sort_values("issue").to_csv(WARNINGS, index=False)
    except Exception as e:
        print(f"  warnings: using cached copy ({e!r})")
    return pd.read_csv(WARNINGS)


def warning_days(wr: pd.DataFrame) -> set[date]:
    """Local dates on which a warning was in effect for any part of 9 a.m.-5 p.m."""
    days = set()
    for _, e in wr.iterrows():
        a = datetime.fromisoformat(str(e["issue"]).replace("Z", "+00:00")).astimezone(MST)
        b = datetime.fromisoformat(str(e["expire"]).replace("Z", "+00:00")).astimezone(MST)
        d = a.date()
        while d <= b.date():
            lo = datetime(d.year, d.month, d.day, 9, tzinfo=MST)
            hi = datetime(d.year, d.month, d.day, 17, tzinfo=MST)
            if a < hi and b > lo:
                days.add(d)
            d += timedelta(days=1)
    return days


# ── analysis ────────────────────────────────────────────────────────────────

def rate(n: int, days: int) -> float | None:
    return round(n / days, 3) if days else None


def main() -> None:
    calls = load_calls()
    last = max(calls["date"])
    w = load_weather(min(last, date.today() - timedelta(days=5)))
    wr = load_warnings()
    wdays = warning_days(wr)

    first = min(calls["date"])
    cal = pd.DataFrame({"date": pd.date_range(first, last, freq="D").date})
    cal = cal.merge(w[["date", "temperature_2m_max"]], on="date", how="left")
    cal["warning"] = cal["date"].isin(wdays)
    cal["year"] = [d.year for d in cal["date"]]
    cal["month"] = [d.month for d in cal["date"]]
    cal["post"] = cal["date"] >= PROGRAM_START
    cal["season"] = cal["month"].between(5, 9)
    calls = calls.merge(cal, on="date", how="left")

    rep: dict = {"zone": ZONE, "program_start": PROGRAM_START.isoformat(),
                 "span": [first.isoformat(), last.isoformat()],
                 "calls": int(len(calls)),
                 "groups": calls.groupby("group").size().sort_values(ascending=False).to_dict(),
                 "weather_days": int(cal["temperature_2m_max"].notna().sum())}

    # Warning days per year (cross-check: city review says 20, 18, 42, 45 for 2021-24)
    wy = pd.Series([d.year for d in wdays]).value_counts().sort_index()
    rep["warning_days_by_year"] = {int(k): int(v) for k, v in wy.items()}
    hot = cal[cal["temperature_2m_max"] >= 110].groupby("year").size()
    rep["days_110_by_year"] = {int(k): int(v) for k, v in hot.items()}

    # 1. Calls per day by daily high, by kind
    bins = [-100, 70, 80, 90, 100, 105, 110, 200]
    labels = ["<70", "70s", "80s", "90s", "100–104", "105–109", "110+"]
    cal["tbin"] = pd.cut(cal["temperature_2m_max"], bins, right=False, labels=labels)
    calls["tbin"] = pd.cut(calls["temperature_2m_max"], bins, right=False, labels=labels)
    temp = []
    for lab in labels:
        nd = int((cal["tbin"] == lab).sum())
        row = {"bin": lab, "days": nd}
        for kind in ("closure", "south", "other"):
            n = int(((calls["tbin"] == lab) & (calls["kind"] == kind)).sum())
            row[kind] = n
            row[f"{kind}_per_100_days"] = round(100 * n / nd, 1) if nd else None
        temp.append(row)
    rep["by_temperature"] = temp

    # 2. Warning days vs other May-Sep days, before and after, by kind
    dd = []
    for post in (False, True):
        for warn in (True, False):
            mask = cal["season"] & (cal["post"] == post) & (cal["warning"] == warn)
            nd = int(mask.sum())
            row = {"period": "2021 on" if post else "2019–2020",
                   "days": "warning days" if warn else "other May–Sep days", "n_days": nd}
            cm = calls["season"] & (calls["post"] == post) & (calls["warning"] == warn)
            for kind in ("closure", "south", "other"):
                n = int((cm & (calls["kind"] == kind)).sum())
                row[kind] = n
                row[f"{kind}_per_100_days"] = round(100 * n / nd, 1) if nd else None
            # closed hours only
            n = int((cm & (calls["kind"] == "closure") & calls["hour"].isin(CLOSE_HOURS)).sum())
            row["closure_in_closed_hours"] = n
            row["closure_in_closed_hours_per_100_days"] = round(100 * n / nd, 1) if nd else None
            dd.append(row)
    rep["warning_vs_not"] = dd

    # Difference in differences: at each kind of trail, how did the warning-day
    # rate move relative to the same trails' ordinary summer days? A ratio of
    # rate ratios, with an approximate 95% interval from the Poisson counts.
    import math
    did = {}
    for kind in ("closure", "south", "other"):
        pre_w, pre_o, post_w, post_o = (next(r for r in dd if r["period"] == p and r["days"] == d)
                                        for p, d in [("2019–2020", "warning days"),
                                                     ("2019–2020", "other May–Sep days"),
                                                     ("2021 on", "warning days"),
                                                     ("2021 on", "other May–Sep days")])
        n = [pre_w[kind], pre_o[kind], post_w[kind], post_o[kind]]
        d = [pre_w["n_days"], pre_o["n_days"], post_w["n_days"], post_o["n_days"]]
        if min(n) == 0:
            continue
        rr_pre = (n[0] / d[0]) / (n[1] / d[1])
        rr_post = (n[2] / d[2]) / (n[3] / d[3])
        se = math.sqrt(sum(1 / x for x in n))
        r = rr_post / rr_pre
        did[kind] = {"warning_vs_ordinary_before": round(rr_pre, 2),
                     "warning_vs_ordinary_after": round(rr_post, 2),
                     "ratio": round(r, 2),
                     "ci95": [round(r * math.exp(-1.96 * se), 2), round(r * math.exp(1.96 * se), 2)],
                     "counts": n}
    rep["diff_in_diff"] = did

    # 3. Hour of day at the closure trails on warning days, before vs after
    hrs = {}
    for post in (False, True):
        m = (calls["kind"] == "closure") & calls["warning"] & (calls["post"] == post)
        h = calls.loc[m, "hour"].value_counts().reindex(range(24), fill_value=0)
        hrs["2021 on" if post else "2019–2020"] = [int(x) for x in h]
    rep["closure_hours_on_warning_days"] = hrs

    # 4. The list of post-program calls at closure trails inside closed hours
    m = (calls["kind"] == "closure") & calls["warning"] & calls["post"] & calls["hour"].isin(CLOSE_HOURS)
    rep["closed_hour_calls"] = [
        {"date": str(r.date), "hour": int(r.hour), "address": r.address, "high": r.temperature_2m_max}
        for r in calls[m].itertuples()]

    # 5. May-Sep calls by year and kind (compare with city-reported counts)
    ys = calls[calls["season"]].groupby(["year", "kind"]).size().unstack(fill_value=0)
    rep["season_by_year"] = {int(y): {k: int(v) for k, v in r.items()} for y, r in ys.iterrows()}
    ya = calls.groupby(["year", "kind"]).size().unstack(fill_value=0)
    rep["all_by_year"] = {int(y): {k: int(v) for k, v in r.items()} for y, r in ya.iterrows()}

    OUT.write_text(json.dumps(rep, indent=1, default=str))
    print(json.dumps({k: v for k, v in rep.items() if k != "closed_hour_calls"}, indent=1, default=str))


if __name__ == "__main__":
    main()
