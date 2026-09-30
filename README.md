# Ridgeline

**Search and rescue at the wildland–urban edge, from public fire-department dispatch data.**

Ridgeline collects every mountain, technical and water rescue call that three fire departments publish, puts each one on a map, and counts them by year, month, hour and trailhead. The goal is to show when people get into trouble near cities built against steep country, and where it happens.

**Live site:** [brooksgroves.com/ridgeline](https://brooksgroves.com/ridgeline/) · **Detailed Phoenix map** (trails, trailheads, preserve boundaries): [brooksgroves.com/ridgeline/map.html](https://brooksgroves.com/ridgeline/map.html)

| City | Agency | Coverage | Mountain & technical rescues located |
|---|---|---|---|
| Phoenix, AZ | Phoenix Fire Department | 2019–2025 | 1,197 of 1,619 (74%) |
| Scottsdale, AZ | Scottsdale Fire Department | Dec 2022 – present | 360 (466 of 522 rescue calls located, 89%) |
| Boulder, CO | Boulder Fire-Rescue | 2015 – present | 581 (published points, 100%) |

Plus a **national parks** section: all 20,058 search-and-rescue incidents the National Park Service logged from 2013 to August 2021, on a map of every park's boundary with one circle per park, and each park's calendar and incidents per million visits.

The city pipeline reruns every Monday, the national parks data monthly, and both whenever the code changes (see [Automation](#automation)). The counts in this README reflect the run of September 29, 2026; the live site always shows the current numbers.

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

**A few trailheads account for most of the calls.** In Phoenix, Echo Canyon on Camelback Mountain (dispatch address `49XX E MCDONALD DR`) accounts for 310 of the 1,197 located mountain rescues, about 26%. Piestewa Peak, Cholla on Camelback, Pima Canyon on South Mountain and the South Mountain Park entrance make up most of the rest. In Boulder, 243 of 581 technical rescues sit at Chautauqua Park, the main entry to the Flatirons.

**Phoenix mountain rescues have fallen by about a fifth.** Counting every call, located or not, there were 255 in 2019 and about 203 a year in 2023–2025. (An earlier version said "about a third", from located calls only; that overstated the drop, because which calls could be located changed over time.) Those years overlap Phoenix's heat-triggered trail closures, but the closures can't be most of the explanation (next paragraph). Other changes over the same years (visitation, dispatch coding, the pandemic) could produce the same shape.

**Heat closures: a signal on closure days, too small to explain the long drop.** Phoenix closes the Echo Canyon, Cholla and Piestewa Peak trails on National Weather Service heat days, and added South Mountain in October 2024. The rules changed several times, and `src/analysis/heat.py` scores every date under the rules in force that day:

| From | Closed | Trigger | Rules |
|---|---|---|---|
| Jul 16, 2021 | 11 a.m.–5 p.m. | Excessive Heat Watch | Pilot through Sep 30: Echo Canyon and Piestewa Peak (Cholla closed for renovation) |
| May 1, 2022 | 11 a.m.–5 p.m. | Warning | May–September |
| May 1, 2023 | 11 a.m.–5 p.m. | Warning | May–September rules |
| Aug 31, 2023 | 9 a.m.–5 p.m. | Warning | Year-round |
| Oct 25, 2024 | 8 a.m.–5 p.m. | Warning | South Mountain added |
| Mar 27, 2025 | 8 a.m.–5 p.m. | Warning | South Mountain limited to Holbert, Mormon, Hau'pal Loop and Pima Canyon access to the National Trail |

The timeline was shared by Yun-Peng (Liz) Lu (University of Maryland) and checked against [2021](https://cronkitenews.azpbs.org/2021/07/15/hiking-trails-on-piestewa-camelback-will-close-when-temperatures-hit-105/) and [2023](https://cronkitenews.azpbs.org/2023/09/08/phoenix-hiking-trails-camelback-mountain-piestewa-peak-heat-warning-closures/) coverage and the city's [2024 release](https://www.phoenix.gov/newsroom/parks-news/3256.html).

The analysis lines up all 1,619 mountain-rescue calls (trail assigned from the dispatch address, so unlocated calls count) with every NWS heat warning and watch for zone AZZ543, Central Phoenix, from the IEM VTEC archive, and Open-Meteo daily highs at Sky Harbor. Counting every date a warning touched gives 20, 18, 42 and 45 days for 2021–2024, exactly the city's 2024 program review. *Closure days* are stricter: the trigger must be in effect during that day's closed hours, inside the program's season (8, 17, 42 and 45 days; 2021 is the pilot only, and 2022 drops July 17, when the warning expired at 2 a.m.). As an independent check, an ABC15 report at the end of the 2021 pilot, found by Yun-Peng Lu, said the trails closed on eight days with no mountain rescues during closure hours: the same 8 days and zero closed-hour rescues this analysis reconstructs. *Before* is January 2019 to July 15, 2021, where a heat day is one with a warning in effect from 11 a.m. to 5 p.m.

| Rescues per 100 days, May–Sep | Before, heat days | Before, ordinary | After, closure days | After, ordinary | Net change (95% range) |
|---|---|---|---|---|---|
| Closure trails | 32 | 34 | 11 | 24 | 0.49 (0.25–0.98) |
| South Mountain (closed from Oct 2024) | 13 | 10 | 8 | 7 | 0.83 (0.31–2.22) |
| Other Phoenix trails | 34 | 32 | 30 | 26 | 1.08 (0.62–1.87) |

Closure days got relatively quieter only at the closed trails, about a 51% relative drop, and the interval only just excludes no effect. The counts are small (25 and 15 calls), so it's a signal, not a measurement. The closure trails' long decline (119 rescues in 2019, about 74 a year in 2023–2025) is year-round, and with about 31 warning days a year the closures could account for about 6 rescues a year at most. Rescues per day don't rise with temperature. Since the pilot began, 17 rescues at closed trails happened during that day's closed hours (none in 2021–22); the site lists them.

**Watches.** A watch later upgraded to a warning is stored in the VTEC archive with its end before its start, so it has no in-effect time of its own. In this zone every 2021 pilot watch was upgraded to a warning covering the same days, so watches add no closure days unless a closure began on the day a watch was *issued* (the product time). That reading isn't used here.

**The city's "rescues on closed trails" figures.** Phoenix's [October 2024 release](https://www.phoenix.gov/newsroom/parks-news/3256.html) reports 57, 47, 30 and 35 rescues on closed trails for 2021–2024, without a definition. Counting every mountain-rescue call at the closure trails from May through October, on any day and at any hour, gives 55, 50, 33 and 36, within 3 a year. So the figures most likely count *trails that close*, not rescues while closed (those are 0, 0, 3 and 9 under the rules in force each day). Over the same years the November–April count at those trails fell just as much (53 to 35), with no closures in effect.

**National parks: a few parks carry most of the load.** From 2016 to 2020 the Park Service logged about 3,050 search-and-rescue incidents a year. Yosemite (~280 a year), Grand Canyon (~260) and Lake Mead (~240) account for 26% of them. Per visitor, Sequoia & Kings Canyon is highest at 90 incidents per million visits, then Yosemite (70) and Grand Canyon (47). Great Smoky Mountains, the most-visited park, has 5.8. July is the busiest month nationally. At Yosemite, June through September is the season, and January is almost silent.

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

**National parks.** The [NPS FOIA reading room](https://www.nps.gov/aboutus/foia/foia-frd.htm) publishes two search-and-rescue incident lists, *NPS-SAR-Incidents-List-2013-2018.xlsx* and *SAR-Incidents-List-2019-2020.xlsx*. The second actually runs to August 12, 2021. Each row is an incident number, date, incident type, park code and region; there is no location inside the park, no time of day and no outcome. `src/ingest/fetch_nps.py` combines them (20,058 incidents after dropping 420 rows with no date, most of which also have no park, and 2 duplicate IDs), maps a few spelled-out park codes (`GRANDCANYON` → `GRCA`), and treats Sequoia & Kings Canyon as one park (`SEKI`) as the incident data does. Park outlines come from the [NPS Land Resources Division boundary service](https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/NPS_Land_Resources_Division_Boundary_and_Tract_Data_Service/FeatureServer/2), and annual recreation visits from the [NPS IRMA visitor use statistics](https://irma.nps.gov/Stats/) service. These are periodic FOIA releases, so `nps.yml` refreshes them monthly rather than weekly, and adds any new SAR list that appears on the FOIA page. To run it locally: `pip install httpx openpyxl`, then `python src/ingest/fetch_nps.py`.

On the map, every park with incidents gets its boundary. Zoomed out, the page draws a light copy snapped to about 2 km (`site/data/nps_overview.geojson`, built from the committed boundaries); from zoom 7 each park in view swaps to its detailed outline (`site/data/nps/<CODE>.geojson`). Ten very small units have no outline at national scale and show as circles only. Clicking a park's circle or boundary opens a popup (incidents a year, rank, rate per million visits, busiest month, weekend share, date range) and loads its charts below the map.

Reporting ramps up over the first years: 570 incidents are dated 2013, 725 in 2014 and 1,230 in 2015, then about 3,000 a year. Yosemite logged 9, 11 and 47 in 2013–2015 and 372 in 2016. That's the reporting system coming into use, not safer parks, so averages and rates use the five complete years, 2016–2020.

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
| Same-street near miss, score 70–80 (same direction and street type) | 683 |
| Preserve representative point | 239 |
| Not located | 422 |
| **Total** (1,619 unique incident IDs) | **1,619** |

### 3. Publish

One GeoJSON file per city is written to `data/external/`, together with a JSON report of counts and match rates. [`src/viz/build_site.py`](src/viz/build_site.py) computes every number on the site directly from those files and renders the page. Its charts are inline SVG, and the map is Leaflet reading the same GeoJSON. Nothing on the site is modeled or estimated.

### Automation

Three workflows keep everything current. None needs a key or secret.

| Workflow | When | What it does |
|---|---|---|
| [`deploy.yml`](.github/workflows/deploy.yml) | Every Monday 06:00 UTC, every push, on demand | Fetches the three cities' dispatch data, filters, geocodes (cache first), exports GeoJSON; refreshes heat warnings and daily highs and reruns the heat analysis; commits the cache, reports and data back; builds the site and deploys it to GitHub Pages |
| [`nps.yml`](.github/workflows/nps.yml) | 1st of each month 07:00 UTC, on demand, and when the fetch code changes | Re-downloads the NPS search-and-rescue lists, park boundaries and visits; **picks up any new SAR list the FOIA page adds**; if anything changed, commits it and starts `deploy.yml` |
| [`probe.yml`](.github/workflows/probe.yml) | On demand | Tests candidate cities' open data and records what they contain in `source_probe.json` |

To refresh by hand: **Actions → workflow → Run workflow** on GitHub.

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
| `data/external/nps_sar_incidents.csv` | NPS SAR incidents: id, date, type, park, region |
| `data/external/nps_parks.geojson`, `nps_visitation.csv` | Park outlines (with a label point) and annual recreation visits |
| `data/external/nps_report.json` | NPS fetch report: rows kept, dropped and why; boundary and visitation matches |
| `data/external/heat_report.json` | Heat analysis: warning days per year, rescue rates by temperature and by warning day, before/after, closed-hour calls |
| `data/external/phoenix_mountain_calls.csv` | All Phoenix mountain-rescue calls: date, hour, dispatch address |
| `data/external/phoenix_heat_warnings.csv`, `phoenix_daily_weather.csv` | NWS heat warnings (zone AZZ543) and daily weather at Sky Harbor |
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

Both are fixed. Mapped Phoenix mountain rescues rose from 474 to 1,159 (1,197 after the follow-up fixes below). The crisis-care and wildland-fire clusters disappeared: 5 crisis calls remain, all at genuine preserve addresses. Every point now records its precision, and match rates are published with each run in `geocode_report.json`. The weekly workflow had also been disabled by GitHub for inactivity, so the map had been a hand-exported snapshot. Geocoding now runs in CI.

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

**Follow-up, late September 2026.**

- **Opposite-direction matches.** A Scottsdale call on *East* Indian School Road had been matched with a high score to *West* Indian School Road, 15 miles away. Matches on the right street with the opposite direction are now rejected (7 of about 1,300).
- **Cholla's new address.** Since the Cholla Trail reopened in 2022, Phoenix Fire logs its calls at `51XX N INVERGORDON RD`, the trailhead corner in Paradise Valley, instead of `62XX E CHOLLA LN`. Those 59 rescues had failed to geocode, and the first run of the heat analysis counted them as "other trails", which overstated the closure-trail decline. Both are fixed.
- **Trend from all calls.** The "down about a third" headline used located calls only. Because the Cholla calls became unlocatable from 2022, that overstated the decline. Trends now use all 1,619 calls: down about a fifth.
- **Closure policy timeline.** The first heat analysis assumed one rule (9 a.m.–5 p.m. on a warning, from May 2021). The program actually began as a July 16, 2021 pilot at 11 a.m.–5 p.m. on a watch, and moved to 9 a.m. only on August 31, 2023. With each date scored under its own rules, the closure-trail net change went from 0.68 (0.34–1.33) to 0.49 (0.25–0.98), and closed-hour rescues from 21 to 17. Thanks to Yun-Peng (Liz) Lu for the timeline and for flagging the watch records.
- **Wrong street type.** Near-miss matches must now also have the same street type. This rejected six wrong matches, including `7XX E DESERT FOOTHILLS PW` → Desert Flower Ln (21 South Mountain rescues, now unplaced rather than misplaced) and Invergordon Rd → Invergordon Pl, three miles north.

---

## Roadmap

- [x] Geocoding fix: street-name matches, hundred blocks, near-miss rule (Sept 2026)
- [x] Geocoding in CI with a committed cache and match report
- [x] Scottsdale and Boulder
- [x] **Closure analysis.** NWS heat-warning days and Open-Meteo daily highs against closure-trail rescues, on the real policy timeline (Sept 2026)
- [ ] **Trail use.** Phoenix's [Hiking Trail Usage](https://www.phoenixopendata.com/dataset/hiking-trail-usage) infrared counter data (2019 on, CC BY) as a denominator: rescues per hiker, not per day
- [ ] Locate the remaining 422 Phoenix rescues (North Mountain, Papago, Desert Foothills) with trailhead-specific matching
- [x] National parks: NPS SAR incidents 2013–2021 by park, with outlines, calendars and per-visit rates (Sept 2026)
- [ ] National parks, deaths: the NPS mortality release (2007–2023) has date, park, cause, intent and outcome per death
- [ ] Sheriff and state SAR logs. Requests are pending with Maricopa County Sheriff's Office and Arizona DEMA
- [ ] More cities with incident-level, located rescue data. Suggestions welcome

---

## Credits and licensing

Data belongs to its publishers: the City of Phoenix (CC BY), the City of Scottsdale (open-data terms), and the City of Boulder (CC0). Geocoding is by Maricopa County GIS; basemap tiles are © Esri. Built by [Brooks Groves](https://brooksgroves.com). Questions or corrections: contact@brooksgroves.com.
