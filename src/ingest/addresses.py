"""
ridgeline / src / ingest / addresses.py

Address handling shared by the fetch and geocode steps.

Two problems this module exists to solve, both found in September 2026:

1. Phoenix Fire publishes addresses masked to the hundred block
   ("11XX E MCDOWELL RD") or as intersections ("N 75TH AV/W MCDOWELL RD").
   The geocoder can't score "11XX" as an address, so most calls failed to
   geocode and silently dropped out. normalize() turns "11XX" into the
   block midpoint "1150" and "A/B" into "A & B", and records the precision
   so downstream code knows a point is block-level, not a front door.

2. Preserve names are also street names. "Camelback", "McDowell" and
   "South Mountain" matched Camelback Road, McDowell Road and South Mountain
   Avenue, so ordinary city calls were pinned to preserve centroids up to
   20 miles away. Preserve matching now uses explicit patterns that mean the
   preserve, never the arterial of the same name.
"""

from __future__ import annotations

import re

# name, pattern, (lat, lon) of a representative point for that preserve.
# Patterns describe the *place*, not a street that borrows its name.
PRESERVE_SITES: list[tuple[str, str, tuple[float, float]]] = [
    ("Camelback Mountain",     r"camelback\s+(mountain|mtn)|echo\s+canyon",      (33.5194, -111.9749)),
    ("Cholla Trailhead",       r"cholla\s+(tr|trl|trail|trailhead)\b",           (33.5244, -111.9603)),
    ("Piestewa Peak",          r"(piestewa|squaw)\s+peak",                                (33.5307, -112.0197)),
    ("Dreamy Draw",            r"dreamy\s+draw",                                  (33.5460, -112.0260)),
    ("North Mountain",         r"north\s+(mountain|mtn)\s+(park|preserve|trail)", (33.5710, -112.0580)),
    ("Shaw Butte",             r"shaw\s+butte(?!\s+(dr|rd|av|ave|st|ln|pl|ct|way)\b)",                                   (33.5791, -112.1020)),
    ("Phoenix Mountains",      r"phoenix\s+(mountain|mtn)s?\s+(park|preserve)",   (33.5550, -112.0200)),
    ("South Mountain Park",    r"south\s+(mountain|mtn)\s+(park|preserve)",       (33.3476, -112.0540)),
    ("Holbert Trailhead",      r"holbert",                                        (33.3476, -112.0540)),
    ("McDowell Sonoran",       r"mcdowell\s+(mountain|mtn|sonoran)",              (33.6918, -111.7951)),
    ("White Tank Mountains",   r"white\s+tank",                                   (33.5971, -112.5476)),
    ("Estrella Mountain",      r"estrella\s+(mountain|mtn)",                      (33.4317, -112.4076)),
    ("Usery Mountain",         r"usery",                                          (33.4754, -111.6218)),
]
_COMPILED = [(n, re.compile(p, re.I), c) for n, p, c in PRESERVE_SITES]

# One regex for the fetch step's address filter.
PRESERVE_PATTERN = re.compile("|".join(f"(?:{p})" for _, p, _ in PRESERVE_SITES), re.I)

_BLOCK = re.compile(r"\b(\d*)XX\b", re.I)

# Phoenix Fire's street-type abbreviations that the county geocoder doesn't
# know, and a street renamed since some of these calls were logged.
_SUFFIX = [(re.compile(r"\bPW\b", re.I), "PKWY"), (re.compile(r"\bAV\b", re.I), "AVE")]
_ALIASES = [(re.compile(r"\bSQUAW\s+PEAK\b", re.I), "PIESTEWA PEAK")]

_DIRS = {"N", "S", "E", "W"}
_TYPES = {"RD", "DR", "ST", "AVE", "AV", "PKWY", "PW", "LN", "WAY", "PL", "CT",
          "BLVD", "CIR", "TRL", "TR", "HWY", "FWY", "LOOP", "TER", "PASS"}


def _clean(part: str) -> str:
    for rx, rep in _SUFFIX + _ALIASES:
        part = rx.sub(rep, part)
    return part


def street_words(query: str) -> list[set[str]]:
    """
    The distinctive words of each street in a query, used to check that a
    geocoder match landed on the street we asked for: '4950 E MCDONALD DR'
    -> [{'MCDONALD'}], 'N 7TH ST & E DUNLAP AVE' -> [{'7TH'}, {'DUNLAP'}].
    """
    out = []
    for part in query.upper().split("&"):
        words = [w for w in re.findall(r"[A-Z0-9]+", part)
                 if not w.isdigit() and w not in _DIRS and w not in _TYPES]
        if words:
            out.append(set(words))
    return out


def preserve_site(address: str) -> tuple[str, tuple[float, float]] | None:
    """Return (name, (lat, lon)) if the address clearly refers to a preserve."""
    if not isinstance(address, str):
        return None
    for name, rx, coords in _COMPILED:
        if rx.search(address):
            return name, coords
    return None


def normalize(address: str) -> tuple[str, str]:
    """
    Turn a Phoenix Fire address into a geocodable query.
    Returns (query, precision) where precision is one of
    'address', 'hundred_block', 'intersection'.
    """
    a = " ".join(str(address).split())
    if "/" in a:
        parts = [_clean(p.strip()) for p in a.split("/") if p.strip()]
        return " & ".join(parts), "intersection"
    a = _clean(a)
    if _BLOCK.search(a):
        # "11XX" -> "1150", "7XX" -> "750", bare "XX" -> "50"
        return _BLOCK.sub(lambda m: (m.group(1) or "") + "50", a, count=1), "hundred_block"
    return a, "address"
