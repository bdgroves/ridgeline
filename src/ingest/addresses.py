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
    # Since the trail reopened in 2022 Phoenix Fire logs Cholla calls at the
    # trailhead corner, 51XX N Invergordon Rd (Paradise Valley), not Cholla Ln.
    ("Cholla Trailhead",       r"cholla\s+(tr|trl|trail|trailhead)\b|\b51(\d\d|XX)\s+N(orth)?\s+invergordon", (33.5244, -111.9603)),
    ("Piestewa Peak",          r"(piestewa|squaw)\s+peak",                                (33.5307, -112.0197)),
    ("Dreamy Draw",            r"dreamy\s+draw",                                  (33.5460, -112.0260)),
    ("North Mountain",         r"north\s+(mountain|mtn)\s+(park|preserve|trail)", (33.5710, -112.0580)),
    ("Shaw Butte",             r"shaw\s+butte(?!\s+(dr|rd|av|ave|st|ln|pl|ct|way)\b)",                                   (33.5791, -112.1020)),
    ("Phoenix Mountains",      r"phoenix\s+(mountain|mtn)s?\s+(park|preserve)",   (33.5550, -112.0200)),
    ("South Mountain Park",    r"south\s+(mountain|mtn)\s+(park|preserve)|\bS\s+(TV|SUMMIT)\s+RD\b",       (33.3476, -112.0540)),  # the park's summit and TV-tower roads
    # Papago Park: Galvin Parkway runs through the park; its hundred blocks have
    # no built addresses. Point = 625 N Galvin Pkwy, a PointAddress match (score
    # 98) from the county geocoder.
    ("Papago Park",            r"\b(6|7|8|9|10|11)XX\s+N\s+GALVIN\s+(PW|PKWY|PARKWAY)\b", (33.4546, -111.9455)),
    ("Holbert Trailhead",      r"holbert",                                        (33.3476, -112.0540)),
    ("McDowell Sonoran",       r"mcdowell\s+(mountain|mtn|sonoran)(?!\s+ranch)",              (33.6918, -111.7951)),
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

_DIRS = {"N", "S", "E", "W", "NORTH", "SOUTH", "EAST", "WEST"}
_TYPES = {"RD", "DR", "ST", "AVE", "AV", "PKWY", "PW", "LN", "WAY", "PL", "CT",
          "BLVD", "CIR", "TRL", "TR", "HWY", "FWY", "LOOP", "TER", "PASS",
          "ROAD", "DRIVE", "STREET", "AVENUE", "PARKWAY", "LANE", "PLACE",
          "COURT", "BOULEVARD", "CIRCLE", "TRAIL", "HIGHWAY", "TERRACE"}


def _clean(part: str) -> str:
    for rx, rep in _SUFFIX + _ALIASES:
        part = rx.sub(rep, part)
    return part


_DIR_LETTER = {"N": "N", "S": "S", "E": "E", "W": "W",
               "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W"}


def _dir_names(text: str) -> dict[str, str]:
    """{street-name word: direction letter} for each 'E INDIAN SCHOOL'-style run."""
    out = {}
    for part in re.split(r"[&,]", text.upper()):
        words = re.findall(r"[A-Z0-9]+", part)
        for i, w in enumerate(words[:-1]):
            if w in _DIR_LETTER:
                nxt = words[i + 1]
                if nxt not in _DIR_LETTER and nxt not in _TYPES:
                    out.setdefault(nxt, _DIR_LETTER[w])
                break
    return out


def direction_conflict(query: str, match_addr: str) -> bool:
    """
    True when the geocoder matched the right street name with the opposite
    direction: 'E INDIAN SCHOOL RD' landing on 'W Indian School Rd' is a
    different place 15 miles away, whatever the score says.
    """
    q, m = _dir_names(query), _dir_names(str(match_addr or ""))
    return any(name in m and m[name] != d for name, d in q.items())


_TYPE_CANON = {"AV": "AVE", "AVENUE": "AVE", "PW": "PKWY", "PARKWAY": "PKWY", "ROAD": "RD",
               "DRIVE": "DR", "STREET": "ST", "LANE": "LN", "PLACE": "PL", "COURT": "CT",
               "BOULEVARD": "BLVD", "CIRCLE": "CIR", "TRAIL": "TRL", "TR": "TRL", "TL": "TRL",
               "HIGHWAY": "HWY", "HW": "HWY", "TERRACE": "TER", "TE": "TER", "BL": "BLVD"}


def _street_types(text: str) -> list[str | None]:
    """The street type of each '&'-separated street, canonicalized, or None."""
    out = []
    for part in re.split(r"[&,]", text.upper())[:2] if "&" in text else [text.upper().split(",")[0]]:
        words = re.findall(r"[A-Z0-9]+", part)
        t = next((w for w in reversed(words) if w in _TYPES or w in _TYPE_CANON), None)
        out.append(_TYPE_CANON.get(t, t) if t else None)
    return out


def type_conflict(query: str, match_addr: str) -> bool:
    """
    True when the match is on a different kind of street with the same name:
    '5150 N INVERGORDON RD' landing on 'N Invergordon Pl', three miles north.
    """
    q, m = _street_types(query), _street_types(str(match_addr or ""))
    return any(a and b and a != b for a, b in zip(q, m))


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
