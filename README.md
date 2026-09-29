# Ridgeline

**Search and rescue at the wildland–urban edge, from public fire-department dispatch data.**

Ridgeline collects every mountain, technical and water rescue call that three fire departments publish, puts each one on a map, and counts them by year, month, hour and trailhead. The goal is to show when people get into trouble near cities built against steep country, and where it happens.

**Live site:** [brooksgroves.com/ridgeline](https://brooksgroves.com/ridgeline/) · **Detailed Phoenix map** (trails, trailheads, preserve boundaries): [brooksgroves.com/ridgeline/map.html](https://brooksgroves.com/ridgeline/map.html)

| City | Agency | Coverage | Mountain & technical rescues located |
|---|---|---|---|
| Phoenix, AZ | Phoenix Fire Department | 2019–2025 | 1,159 of 1,619 (72%) |
| Scottsdale, AZ | Scottsdale Fire Department | Dec 2022 – present | 360 (466 of 522 rescue calls located, 89%) |
| Boulder, CO | Boulder Fire-Rescue | 2015 – present | 581 (published points, 100%) |

The pipeline reruns every Monday and whenever the code changes. The counts in this README reflect the run of September 29, 2026; the live site always shows the current numbers.

---

## Contents

1. [What the data shows](#what-the-data-shows)
2. [Data sources](#data-sources)
3. [Method](#method)
4. [How to read the site](#how-to-read-the-site)
5. [Output files](#output-files)
6. [Limitations](#limitations)
7. [Corrections](#corrections)
8. [Running it yourself](#running-it-yourself)
9. [Repository layout](#repository-layout)
10. [Roadmap](#roadmap)
11. [Credits and licensing](#credits-and-licensing)

---

## What the data shows

These are descriptive patterns in dispatch counts. None of them is a causal claim.

**A few trailheads account for most of the calls.** In Phoenix, Echo Canyon on Camelback Mountain (dispatch address `49XX E MCDONALD DR`) accounts for 310 of the 1,159 located mountain rescues, about 27%. Piestewa Peak, Pima Canyon on South Mountain, Cholla on Camelback and the South Mountain Park entrance make up most of the rest. In Boulder, 243 of 581 technical rescues sit at Chautauqua Park, the main entry to the Flatirons.

**Phoenix mountain rescues have fallen by about a third.** There were 206 in 2019 and about 131 a year in 2023–2025. Those years overlap Phoenix's heat-triggered trail closures at Echo Canyon and Cholla. That makes the closures an obvious hypothesis to test, not a conclusion. Other changes over the same years (visitation, dispatch coding, the pandemic) could produce the same shape.

**Each city has its own season:**

| | Busiest month | Quietest month | Share on weekends |
|---|---|---|---|
| Phoenix | March | December | 39.7% |
| Scottsdale | February | June | 41.4% |
| Boulder | July | January | 41.5% |

Boulder follows the classic mountain calendar. Scottsdale is the reverse: winter and spring are busy, and summer goes quiet as people avoid the heat. Phoenix barely has a season at all; March and May are as busy as July. Across all three cities about 40% of calls fall on Saturday or Sunday, which make up 29% of the week. Calls peak late morning in Phoenix and around midday in Boulder. Scottsdale publishes dates without times.

---

## Data sources

All three sources are public and need no key. Each is pulled fresh on every run.

| | Phoenix | Scottsdale | Boulder |
|---|---|---|---|
| Dataset | [Fire calls for service](https://www.phoenixopendata.com/dataset/caf49f72-f22f-4ad9-9405-2a3db9619423), one CSV per year | [Fire Department calls for service](https://data.scottsdaleaz.gov/datasets/187dd861ed1c4aecb392fce2ee901c05_20) (ArcGIS layer) | [Response times for Boulder Fire-Rescue](https://open-data.bouldercolorado.gov/datasets/18c58aed5261498980e61b1e58eed376_0) (ArcGIS layer) |
| License | CC BY | City of Scottsdale open-data terms | CC0 1.0 |
| Unit | One row per call | One row per call (deduplicated on incident number) | One row per incident |
| Location | Address masked to the hundred block (`49XX E MCDONALD DR`) or an intersection (`N 7TH ST/E DUNLAP AV`) | Full street address | Point geometry, no address |
| Time | Date and time | Date only | Date and time |
| What counts as mountain / technical | Nature text *Mountain Rescue*, plus tree, confined-space and rescue-service calls | Department code `MTNRES`, plus *High-angle rescue* | Call types *Rescue*, *Auto-Aid Tech Rescue*, *Auto-Aid Rescue*; NFIRS *High-angle rescue* and land searches |
| What counts as water | *Water Rescue*, *Check Flooding Condition*, swift-water calls | Code `WATER`, *Swift water rescue* | *Water Rescue* call types; NFIRS swift-water, ice and in-water search codes |

Scottsdale's land searches (*Search for person on land* outside the `MTNRES` code) are kept as a separate **land search** category. Many of them are urban missing-person calls rather than trail work. Boulder's *auto-aid* calls are Boulder units sent outside the city, often into county open space.

Other cities were checked and set aside. Santa Barbara (city and county) doesn't publish incident-level fire calls. San Diego publishes a generic *RESCUE* category with only a ZIP code for location, which is too coarse to map. The probe that tested these sources is in `tools/probe_sources.py`, and its last results are in `data/external/source_probe.json`.

---

## Method

```
Phoenix CSVs ─┐                                        ┌─ phoenix_sar_incidents.geojson
Scottsdale ───┼─ filter rescue call types ─ locate ─────┼─ scottsdale_sar_incidents.geojson ─┐
Boulder ──────┘                                        └─ boulder_sar_incidents.geojson     ├─ site/index.html
                         geocode_cache.csv ◄──► Maricopa County geocoder                    │
                         *_report.json  (match rates, counts)  ─────────────────────────────┘
```

### 1. Filter

- **Phoenix:** calls are kept when the nature text matches a rescue, flood or heat term, or when the address names a preserve or trailhead. Preserve matching uses patterns that mean the place (*Echo Canyon*, *Piestewa Peak*, *McDowell Sonoran*), never an arterial that shares its name. The patterns are in [`src/ingest/addresses.py`](src/ingest/addresses.py).
- **Scottsdale and Boulder:** rescue call types are selected on the server with an ArcGIS query.

### 2. Locate

Boulder publishes points, so nothing there is geocoded. Phoenix and Scottsdale addresses go to the [Maricopa County geocoder](https://gis.maricopa.gov/) in these steps:

1. **Normalize the address.** Hundred blocks become the block midpoint (`49XX` → `4950`). Intersections become `A & B`. Phoenix Fire's abbreviations are expanded (`PW` → `PKWY`, `AV` → `AVE`). A renamed street is updated (*Squaw Peak Dr* → *Piestewa Peak Dr*).
2. **Accept a match scoring 80 or higher.**
3. **Accept a near miss (score 70–80) only when it lands on the same street.** Hundred-block midpoints often fall outside a street's address range, which lowers the score even when the street is right. So a near miss is kept only if it is an address, street or intersection match and the geocoder's matched address contains the street name that was asked for.
4. **Fall back to a preserve.** If the geocoder fails and the address clearly names a preserve, the call is placed at a representative point for that preserve.
5. **Otherwise the call is not located.** It stays in the counts that note it, but it isn't on the map.

Every result is written to `data/external/geocode_cache.csv`, so the next run only queries addresses it hasn't seen. The cache records the query, score, matched address, match type, method and a logic version. Misses from an older logic version are retried; hits are kept.

For Phoenix mountain rescues, the 1,619 calls resolve like this:

| Outcome | Calls |
|---|---:|
| Geocoder match, score ≥ 80 | 275 |
| Same-street near miss, score 70–80 | 704 |
| Preserve representative point | 180 |
| Not located | 460 |
| **Total** (1,619 unique incident IDs) | **1,619** |

### 3. Publish

One GeoJSON file per city is written to `data/external/`, together with a JSON report of counts and match rates. [`src/viz/build_site.py`](src/viz/build_site.py) computes every number on the site directly from those files and renders the page. Its charts are inline SVG, and the map is Leaflet reading the same GeoJSON. Nothing on the site is modeled or estimated.

### Automation

[`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) runs every Monday and on every push. It fetches, filters, geocodes, exports and builds, then commits the updated cache, reports and GeoJSON back to the repo and deploys the site to GitHub Pages.

---

## How to read the site

- **A circle is a place, not a call.** Calls that share a location are drawn as one circle, and a bigger circle means more calls. Click a circle to see its call count, date range, and how its location was determined.
- **Map layers.** The map opens showing mountain and technical rescues only. Use the buttons under the map to add flood and water rescues or Scottsdale's land searches.
- **Location precision.** Each circle is labeled with how precisely it is placed:

  | Label | Meaning | Typical accuracy |
  |---|---|---|
  | Middle of the hundred block | Phoenix address masked to the block | within ~50 m along the street |
  | Intersection named in the call | The call gave two streets | the intersection itself |
  | Street address | Scottsdale full address | the parcel frontage |
  | Point published by the city | Boulder's own location | as published; many calls share one point |
  | Preserve (representative point) | Only the preserve was known | anywhere in that preserve |

- **Counts are dispatches, not people or outcomes.** A call can involve one hiker or a whole group. Some calls were cancelled en route or found no one.
- **Compare shapes, not totals, across cities.** Each department codes rescues differently, and county sheriff's teams and volunteer groups handle many backcountry rescues that never appear in city fire data.
- **Faded bars are partial years.** Totals for those years aren't comparable to full years.

---

## Output files

Everything the site needs is committed, so the site can be rebuilt without re-downloading anything.

| File | Contents |
|---|---|
| `data/external/{city}_sar_incidents.geojson` | One point per located call |
| `data/external/geocode_report.json` | Phoenix counts, match rates by method and precision, top unlocated addresses |
| `data/external/scottsdale_report.json`, `boulder_report.json` | Per-city counts, categories and date ranges |
| `data/external/geocode_cache.csv` | Every address ever looked up, and how it resolved |
| `data/external/source_probe.json` | Schema and vocabulary check of candidate sources |

GeoJSON feature properties:

| Property | Description |
|---|---|
| `incident_type` | The call type or description as published |
| `category` | `mountain`, `water` or `search` (Scottsdale and Boulder; for Phoenix the site derives it from `incident_type`) |
| `date`, `year`, `hour` | Local date and time of the call (`hour` is empty for Scottsdale) |
| `is_weekend` | Saturday or Sunday |
| `location_name` | The dispatch address as published (empty for Boulder) |
| `precision` | `hundred_block`, `intersection`, `address`, `preserve_centroid` or `published_point` |

---

## Limitations

- **Unlocated calls.** 28% of Phoenix mountain rescues aren't on the map. The largest unmatched addresses are `106XX N 7TH ST` (North Mountain), `51XX N INVERGORDON RD` and `N GALVIN PW` (Papago Park). The county geocoder doesn't resolve them.
- **Near-miss matches are street-accurate, not door-accurate.** They are checked against the street name, but the point is the geocoder's position along that street.
- **Preserve points are area-level.** 180 Phoenix mountain rescues sit at a preserve's representative point.
- **Boulder shares points.** 99 calls share a single point near Bear Peak, which is probably a stand-in location for backcountry calls. Treat Boulder's map as area-level outside Chautauqua.
- **Coverage differs by city.** Phoenix is 2019–2025, Scottsdale starts in December 2022, and Boulder starts in 2015. Scottsdale has no time of day.
- **Fire-department calls only.** Sheriff's offices, volunteer teams and land agencies run many backcountry rescues, and none of those appear here.

---

## Corrections

**September 2026.** An earlier version reported 2,263 Phoenix incidents, led by **Crisis / Behavioral Health (1,120)**, which it described as concentrated "along the preserve edges," plus a **Wildland Fire (186)** cluster. Both were artifacts of two bugs:

1. **Street names matched as preserves.** The preserve-name fallback matched street names: *McDowell*, *Camelback* and *South Mountain* also matched McDowell Road, Camelback Road and South Mountain Avenue. That pulled ordinary city calls on those arterials into the dataset and pinned them to preserve centroids up to 20 miles away. About 1,560 of the 2,263 old points sat on four such centroids.
2. **Hundred-block addresses didn't geocode.** The geocoder couldn't score hundred-block addresses or `A/B` intersections, so most real mountain rescues dropped out without any error. Only 474 of 1,619 reached the map.

Both are fixed. Mapped Phoenix mountain rescues rose from 474 to 1,159. The crisis-care and wildland-fire clusters disappeared: 5 crisis calls remain, all at genuine preserve addresses. Every point now records its precision, and match rates are published with each run in `geocode_report.json`. The weekly workflow had also been disabled by GitHub for inactivity, so the map had been a hand-exported snapshot. Geocoding now runs in CI.

---

## Running it yourself

Requires [pixi](https://pixi.sh). Everything else, including Python, comes from pixi.

```bash
git clone https://github.com/bdgroves/ridgeline.git
cd ridgeline
pixi install

pixi run phoenix      # download Phoenix Fire CSVs and filter rescue calls
pixi run geocode      # geocode Phoenix (uses and updates the committed cache)
pixi run export       # write phoenix_sar_incidents.geojson
pixi run scottsdale   # fetch + geocode Scottsdale
pixi run boulder      # fetch Boulder
pixi run build        # build site/ from the committed GeoJSON
pixi run serve        # http://localhost:8080
```

`pixi run build` works on a fresh clone with no downloads, because it reads only the committed files. The first full geocoding run takes a few minutes; later runs only query new addresses.

---

## Repository layout

```
ridgeline/
├── .github/workflows/
│   ├── deploy.yml             weekly pipeline → GitHub Pages
│   └── probe.yml              one-off source probe
├── src/
│   ├── ingest/
│   │   ├── fetch_phoenix_fire.py   Phoenix CSVs → filtered calls
│   │   ├── geocode_phoenix.py      geocoder, cache, match report
│   │   ├── addresses.py            normalization + preserve patterns
│   │   ├── fetch_scottsdale.py     Scottsdale ArcGIS → geocoded GeoJSON
│   │   ├── fetch_boulder.py        Boulder ArcGIS → GeoJSON
│   │   ├── arcgis.py               ArcGIS paging + GeoJSON writer
│   │   └── fetch_gis.py            trails, trailheads, parks for the Phoenix map
│   ├── analysis/                   exploratory plots, weather join, model (not on the site)
│   └── viz/
│       ├── build_site.py           site/index.html
│       └── build_map.py            site/map.html (detailed Phoenix map)
├── export_geojson.py               Phoenix geocoded calls → GeoJSON
├── tools/probe_sources.py          checks candidate open-data sources
└── data/external/                  committed outputs (see above)
```

---

## Roadmap

- [x] Geocoding fix: street-name matches, hundred blocks, near-miss rule (Sept 2026)
- [x] Geocoding in CI with a committed cache and match report
- [x] Scottsdale and Boulder
- [ ] **Closure analysis.** Line up Phoenix's heat-closure dates against Echo Canyon and Cholla calls, with Open-Meteo daily highs as the control
- [ ] Locate the remaining 460 Phoenix rescues (North Mountain, Papago, Invergordon) with trailhead-specific matching
- [ ] Sheriff and state SAR logs. Requests are pending with Maricopa County Sheriff's Office and Arizona DEMA
- [ ] More cities with incident-level, located rescue data. Suggestions welcome

---

## Credits and licensing

Data belongs to its publishers: the City of Phoenix (CC BY), the City of Scottsdale (open-data terms), and the City of Boulder (CC0). Geocoding is by Maricopa County GIS; basemap tiles are © Esri. Built by [Brooks Groves](https://brooksgroves.com). Questions or corrections: contact@brooksgroves.com.
