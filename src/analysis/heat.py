"""
ridgeline / src / analysis / heat.py

Heat and trail closures in Phoenix.

Phoenix closes its busiest mountain trails on National Weather Service heat
days. The rules changed several times (POLICY below): a July 2021 pilot at
11 a.m.-5 p.m. triggered by an Excessive Heat Watch; May-September seasons at
11-5 on Warnings in 2022-23; year-round from 9 a.m. from 31 Aug 2023; 8 a.m.
with South Mountain added from 25 Oct 2024; South Mountain narrowed to named
trails from 27 Mar 2025. Timeline from a researcher's reconstruction (Yun-Peng
Lu, University of Maryland), checked against contemporary news coverage.

This script asks what the dispatch data can say about that:

  * mountain-rescue calls per day by daily high temperature,
  * on heat days, closure trails vs trails that never closed, before the
    program (Jan 2019 - 15 Jul 2021) and after,
  * on closure days, whether calls at the closed trails fall inside or
    outside the closed hours in force on that date.

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
PROGRAM_START = date(2021, 7, 16)      # pilot began Friday 16 July 2021
CLOSE_END = 17                         # every version closes until 5 p.m.
PRE_START_HOUR = 11                    # "would-be" window for heat days before the program

# (start, end or None, closure start hour, trigger, trail kinds closed, note)
POLICY = [
    (date(2021, 7, 16), date(2021, 9, 30), 11, "watch or warning", {"closure"},
     "Pilot: Echo Canyon and Piestewa Peak, 11 a.m.-5 p.m., on an Excessive Heat Watch. Cholla was closed for renovation."),
    (date(2022, 5, 1), date(2022, 9, 30), 11, "warning", {"closure"},
     "May-September, 11 a.m.-5 p.m., on an Excessive Heat Warning."),
    (date(2023, 5, 1), date(2023, 8, 30), 11, "warning", {"closure"},
     "May-September rules, 11 a.m.-5 p.m."),
    (date(2023, 8, 31), date(2024, 10, 24), 9, "warning", {"closure"},
     "Year-round, 9 a.m.-5 p.m., on a Warning (Parks Board, 31 Aug 2023)."),
    (date(2024, 10, 25), date(2025, 3, 26), 8, "warning", {"closure", "south"},
     "8 a.m.-5 p.m.; South Mountain added (Parks Board, 25 Oct 2024)."),
    (date(2025, 3, 27), None, 8, "warning", {"closure", "south"},
     "South Mountain limited to Holbert, Mormon, Hau'pal Loop and Pima Canyon access to the National Trail."),
]


def policy_on(d: date):
    for p in POLICY:
        if p[0] <= d and (p[1] is None or d <= p[1]):
            return p
    return None

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
        # Warnings (W) and watches (A). A watch later upgraded to a warning has
        # an "expire" (the upgrade time) before its "issue" (its valid start);
        # those rows carry no in-effect interval of their own and are kept only
        # for the record. product_id holds the issuance time.
        rows = [{"eventid": e["eventid"], "phenomena": e["phenomena"],
                 "significance": e.get("significance"), "issue": e["issue"],
                 "expire": e["expire"], "name": e.get("name"), "product_id": e.get("product_id")}
                for e in ev if e.get("phenomena") in ("EH", "XH") and e.get("significance") in ("W", "A")]
        pd.DataFrame(rows).sort_values("issue").to_csv(WARNINGS, index=False)
    except Exception as e:
        print(f"  warnings: using cached copy ({e!r})")
    wr = pd.read_csv(WARNINGS)
    if "significance" not in wr.columns:       # older cached copy: warnings only
        wr["significance"] = "W"
    return wr


def intervals(wr: pd.DataFrame, sig: tuple[str, ...]) -> list[tuple[datetime, datetime]]:
    out = []
    for _, e in wr[wr["significance"].isin(sig)].iterrows():
        a = datetime.fromisoformat(str(e["issue"]).replace("Z", "+00:00")).astimezone(MST)
        b = datetime.fromisoformat(str(e["expire"]).replace("Z", "+00:00")).astimezone(MST)
        if b > a:
            out.append((a, b))
    return out


def in_effect(iv, d: date, lo_hour: int = 0, hi_hour: int = 24) -> bool:
    lo = datetime(d.year, d.month, d.day, tzinfo=MST) + timedelta(hours=lo_hour)
    hi = datetime(d.year, d.month, d.day, tzinfo=MST) + timedelta(hours=hi_hour)
    return any(a < hi and b > lo for a, b in iv)


def warning_days(wr: pd.DataFrame) -> set[date]:
    """Local dates a warning touched at any hour (how the city's review counts them)."""
    days = set()
    for a, b in intervals(wr, ("W",)):
        d = a.date()
        while d <= b.date():
            if in_effect([(a, b)], d):
                days.add(d)
            d += timedelta(days=1)
    return days


def day_flags(wr: pd.DataFrame, d: date) -> dict:
    """
    For one date: the policy in force, whether it was a closure day (trigger in
    effect during that day's closed hours), and for dates before the program,
    whether it would have been a heat day under the first rule (11 a.m.-5 p.m.).
    """
    W = intervals(wr, ("W",))
    WA = intervals(wr, ("W", "A"))
    p = policy_on(d)
    if p:
        iv = WA if p[3] == "watch or warning" else W
        closed = in_effect(iv, d, p[2], CLOSE_END)
        return {"policy": p, "heat": closed, "closed": closed, "start_hour": p[2], "kinds": p[4]}
    heat = in_effect(W, d, PRE_START_HOUR, CLOSE_END)
    return {"policy": None, "heat": heat, "closed": False, "start_hour": PRE_START_HOUR, "kinds": set()}


# ── trail use: rescues per counted hiker ───────────────────────────────────
# Phoenix "Hiking Trail Usage" open data: daily infrared counter passes. Two
# counters sit on the closure trails for the whole period, Echo Canyon and the
# Piestewa Summit Trail; Cholla's counter is too patchy (and the trail was shut
# for renovation in 2020-22), so Cholla calls and counts are left out here.
COUNTS = EXT / "phoenix_trail_counts.csv"
COUNTERS = {"echo": "E - Camelback - Echo Canyon Trail", "pies": "E - PMP - Piestewa Summit Trail"}
COUNTER_CALLS = {"echo": r"\bMCDONALD\s+DR\b|ECHO\s+CANYON", "pies": r"(PIESTEWA|SQUAW)\s+PEAK"}


def trail_use(calls: pd.DataFrame, cal: pd.DataFrame) -> dict | None:
    if not COUNTS.exists():
        return None
    d = pd.read_csv(COUNTS)
    d["Site"] = d["Site"].astype(str).str.strip()
    d["date"] = pd.to_datetime(d["Date"], format="%m/%d/%Y", errors="coerce").dt.date
    d["Count"] = pd.to_numeric(d["Count"], errors="coerce")
    piv = d[d["Site"].isin(COUNTERS.values())].pivot_table(index="date", columns="Site", values="Count", aggfunc="sum")
    piv = piv.rename(columns={v: k for k, v in COUNTERS.items()})
    if not set(COUNTERS) <= set(piv.columns):
        return None
    # Days both counters reported something. Zeros are dropped as outages.
    piv = piv.dropna()
    piv = piv[(piv["echo"] > 0) & (piv["pies"] > 0)]
    piv["passes"] = piv["echo"] + piv["pies"]
    rx = {k: re.compile(v, re.I) for k, v in COUNTER_CALLS.items()}
    cc = calls[calls["address"].map(lambda a: any(r.search(str(a)) for r in rx.values()))]
    resc = cc.groupby("date").size()
    x = piv.join(resc.rename("rescues"), how="left").fillna({"rescues": 0})
    x = x.join(cal.set_index("date")[["warning", "post", "temperature_2m_max"]], how="left")
    x["year"] = [dd.year for dd in x.index]
    x["month"] = [dd.month for dd in x.index]
    x["half"] = ["Nov–Apr" if m in (11, 12, 1, 2, 3, 4) else "May–Oct" for m in x["month"]]

    def agg(df):
        n, p, r = len(df), float(df["passes"].sum()), int(df["rescues"].sum())
        return {"days": n, "passes_per_day": round(p / n) if n else None, "rescues": r,
                "per_100k": round(r / p * 1e5, 2) if p else None}

    out = {"counters": COUNTERS, "coverage_days": int(len(x)),
           "span": [str(min(x.index)), str(max(x.index))] if len(x) else None,
           "by_year": {int(y): agg(g) for y, g in x.groupby("year")},
           "by_year_half": {f"{int(y)} {h}": agg(g) for (y, h), g in x.groupby(["year", "half"])}}
    s = x[x["month"].between(5, 9)]
    out["heat"] = {f"{'after' if post else 'before'} {'heat' if w else 'ordinary'}": agg(g)
                   for (post, w), g in s.groupby(["post", "warning"])}
    bins = [-100, 70, 80, 90, 100, 105, 110, 200]
    labels = ["<70", "70s", "80s", "90s", "100–104", "105–109", "110+"]
    x["tbin"] = pd.cut(x["temperature_2m_max"], bins, right=False, labels=labels)
    out["by_temperature"] = [dict(bin=lab, **agg(x[x["tbin"] == lab])) for lab in labels]
    return out


def call_types() -> dict | None:
    """All call types at the closure trailheads by year (from the raw city data)."""
    p = EXT / "trailhead_call_types.csv"
    if not p.exists():
        return None
    t = pd.read_csv(p)
    t = t[t["address"].str.contains(r"MCDONALD\s+DR|ECHO\s+CANYON|CHOLLA\s+LN|51XX\s+N\s+INVERGORDON|(?:PIESTEWA|SQUAW)\s+PEAK",
                                    case=False, regex=True)]
    t["kind"] = t["nature"].str.lower().map(lambda n: "mountain rescue" if n == "mountain rescue" else n)
    top = t.groupby("kind")["calls"].sum().sort_values(ascending=False).head(12).index
    tab = t[t["kind"].isin(top)].pivot_table(index="kind", columns="year", values="calls", aggfunc="sum", fill_value=0)
    return {"by_type": {k: {int(y): int(v) for y, v in r.items()} for k, r in tab.iterrows()},
            "all_calls": {int(y): int(v) for y, v in t.groupby("year")["calls"].sum().items()}}


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
    flags = {d: day_flags(wr, d) for d in cal["date"]}
    # "warning" = a heat day: a closure day after the program started, or a
    # would-be closure day (warning in effect 11 a.m.-5 p.m.) before it.
    cal["warning"] = [flags[d]["heat"] for d in cal["date"]]
    cal["closed_day"] = [flags[d]["closed"] for d in cal["date"]]
    cal["year"] = [d.year for d in cal["date"]]
    cal["month"] = [d.month for d in cal["date"]]
    cal["post"] = cal["date"] >= PROGRAM_START
    cal["season"] = cal["month"].between(5, 9)
    calls = calls.merge(cal, on="date", how="left")
    # A call is "during closure" when its trail was closed that day and its hour
    # falls inside that day's closed window.
    calls["during_closure"] = [
        bool(flags[r.date]["closed"] and r.kind in flags[r.date]["kinds"]
             and flags[r.date]["start_hour"] <= r.hour < CLOSE_END)
        for r in calls.itertuples()]
    calls["in_window"] = [flags[r.date]["start_hour"] <= r.hour < CLOSE_END for r in calls.itertuples()]

    rep: dict = {"zone": ZONE, "program_start": PROGRAM_START.isoformat(),
                 "span": [first.isoformat(), last.isoformat()],
                 "calls": int(len(calls)),
                 "groups": calls.groupby("group").size().sort_values(ascending=False).to_dict(),
                 "weather_days": int(cal["temperature_2m_max"].notna().sum())}

    # Warning days per year. Counting any day a warning touched reproduces the
    # city's review (20, 18, 42, 45 for 2021-24). Closure days need the trigger in
    # force during that day's closed hours, which drops e.g. 17 Jul 2022 (a
    # warning that expired at 2 a.m.).
    wy = pd.Series([d.year for d in wdays]).value_counts().sort_index()
    rep["warning_days_by_year"] = {int(k): int(v) for k, v in wy.items()}
    cy = cal[cal["closed_day"]].groupby("year").size()
    rep["closure_days_by_year"] = {int(k): int(v) for k, v in cy.items()}
    rep["policy"] = [{"from": p[0].isoformat(), "to": p[1].isoformat() if p[1] else None,
                      "hours": f"{p[2]}:00-17:00", "trigger": p[3], "kinds": sorted(p[4]), "note": p[5]}
                     for p in POLICY]
    watch = wr[wr["significance"] == "A"]
    rep["watches"] = {"rows": int(len(watch)),
                      "upgraded_no_interval": int((watch["expire"] < watch["issue"]).sum()) if len(watch) else 0}
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
            row = {"period": "after" if post else "before",
                   "days": "warning days" if warn else "other May–Sep days", "n_days": nd}
            cm = calls["season"] & (calls["post"] == post) & (calls["warning"] == warn)
            for kind in ("closure", "south", "other"):
                n = int((cm & (calls["kind"] == kind)).sum())
                row[kind] = n
                row[f"{kind}_per_100_days"] = round(100 * n / nd, 1) if nd else None
            # inside that day's closed window (or the 11-5 would-be window before)
            n = int((cm & (calls["kind"] == "closure") & calls["in_window"]).sum())
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
                                        for p, d in [("before", "warning days"),
                                                     ("before", "other May–Sep days"),
                                                     ("after", "warning days"),
                                                     ("after", "other May–Sep days")])
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
        hrs["after" if post else "before"] = [int(x) for x in h]
    rep["closure_hours_on_warning_days"] = hrs

    # 4. The list of post-program calls at closure trails inside closed hours
    m = calls["during_closure"]
    rep["closed_hour_calls"] = [
        {"date": str(r.date), "hour": int(r.hour), "address": r.address, "high": r.temperature_2m_max,
         "window": f"{flags[r.date]['start_hour']}:00-17:00"}
        for r in calls[m].itertuples()]

    # 5. May-Sep calls by year and kind (compare with city-reported counts)
    ys = calls[calls["season"]].groupby(["year", "kind"]).size().unstack(fill_value=0)
    rep["season_by_year"] = {int(y): {k: int(v) for k, v in r.items()} for y, r in ys.iterrows()}
    ya = calls.groupby(["year", "kind"]).size().unstack(fill_value=0)
    rep["all_by_year"] = {int(y): {k: int(v) for k, v in r.items()} for y, r in ya.iterrows()}

    # 6. Reconcile the city's published "rescues on closed trails" (2021-24: 57, 47,
    # 30, 35; Parks news release, 25 Oct 2024). The definition isn't published. The
    # closest reproduction is every mountain-rescue call at the closure trails from
    # May through October, any day and any hour: trails that close, not rescues
    # while closed. Nov-Apr at the same trails shows what the off-season did.
    cl = calls["kind"] == "closure"
    heat_season = calls["month"].between(5, 10)
    rep["city_figures"] = {
        "published": {2021: 57, 2022: 47, 2023: 30, 2024: 35},
        "source": "https://www.phoenix.gov/newsroom/parks-news/3256.html",
        "ours_may_oct": {int(y): int(n) for y, n in calls[cl & heat_season].groupby("year").size().items()},
        "ours_nov_apr": {int(y): int(n) for y, n in calls[cl & ~heat_season].groupby("year").size().items()},
        "ours_closed_hours_on_warning_days": {int(y): int(n) for y, n in calls[
            cl & calls["during_closure"]].groupby("year").size().items()},
    }

    rep["trail_use"] = trail_use(calls, cal)
    rep["call_types"] = call_types()

    OUT.write_text(json.dumps(rep, indent=1, default=str))
    print(json.dumps({k: v for k, v in rep.items() if k != "closed_hour_calls"}, indent=1, default=str))


if __name__ == "__main__":
    main()
