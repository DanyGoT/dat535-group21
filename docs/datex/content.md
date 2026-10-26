# DATEX: what the payloads contain

The structure of NPRA's DATEX II 3 publications as we receive them, how they link together,
the traps, and what Silver should do with them. Access, feeds and sizes:
[datex/sources.md](sources.md). From real snapshots of every feed, 2026-10-23 and 2026-10-26.

## Structure

Every payload is one `ns2:messageContainer` (namespaces `http://datex2.eu/schema/3/...`) with
one `payload` element that carries its own `publicationTime`.

| Feed | Inside the payload |
| ---- | ------------------ |
| situation | ~1,150 `situation`, each with `situationVersionTime`, `overallSeverity` and one or more `situationRecord` (~2,500 in total) |
| traveltime | 404 `physicalQuantity`: two per road section, each `TravelTimeData` for a 5-minute period |
| weather | 475 `siteMeasurements` (one per station: `measurementSiteReference`, `measurementTimeDefault`), ~3,770 `physicalQuantity` |
| traveltime-locations | 202 `predefinedLocationReference` (id, name, polyline) |
| weather-sites | one `measurementSiteTable` with 475 `measurementSite` (id, name, point, road number, county, ~11 measurement types each) |

## How the feeds link (verified 2026-10-26)

| Measurement | Points to | Match |
| ----------- | --------- | ----- |
| traveltime `pertinentLocation/predefinedLocationReference/@id` (e.g. `100357`) | traveltime-locations `predefinedLocationReference/@id` | all 202 ids found |
| weather `measurementSiteReference/@id` (e.g. `1629006`) | weather-sites `measurementSite/@id` | all 475 ids found |
| weather `physicalQuantity/@index` (e.g. `101` temperature, `201` humidity, `701` precipitation) | the site's `measurementSpecificCharacteristics/@index` | per site |

- **Travel time** gives `travelTime` and `freeFlowTravelTime` in seconds for each section and
  5-minute period (e.g. 123 s against 150 s), plus a trend. Their ratio is a ready-made
  congestion index. Sections have names like "Åsanevegen - Eikås" (Bergen).
- **Weather sites** have a name ("Fv 714 Våvatnet"), a road number, a county, and a point in
  WGS84 latitude/longitude (58.1–71.2 °N, 5.0–31.0 °E: all of Norway).

## Situation records

- Each `situation` has a unique id (e.g. `NPRA_HBT_30-08-2021.06`); each `situationRecord` a
  unique id with a `version`, and a type (`xsi:type`). Seen on 2026-10-26: `MaintenanceWorks`
  1,170, `RoadOrCarriagewayOrLaneManagement` 609, `GeneralNetworkManagement` 504,
  `SpeedManagement` 225, `ReroutingManagement` 152, then a long tail (obstructions, animals,
  public events). No `Accident` at that moment.
- To pick the latest version, NPRA recommends `situationRecordVersionTime`.
- A record whose `overallEndTime` has passed is expired; no `overallEndTime` means "until
  further notice".
- Records carry `situationRecordCreationTime` and `overallStartTime`, so we know when a
  situation started even if we first saw it later.
- Long-running road works dominate: the oldest record seen was created 2023-08-09.
- Locations: 3,672 WGS84 points (`latitude`/`longitude`) and 2,668 `gmlLineString` lines.

## Pitfalls

- **Three coordinate systems.** Weather sites and situation points are WGS84 lat/lon. Travel
  time sections are polylines in **EPSG:32633** (UTM 33N, metres), situation lines in
  **EPSG:25833** (ETRS89 / UTM 33N). Project before joining to quays or vehicle positions.
  `pyproj` is not installed on the VM.
- **Spark's JSON reader caps strings at 20,000,000 characters** (Jackson 2.15 in Spark 3.5)
  and marks longer lines corrupt. A situation line is ~28.6M. Both Bronze jobs read with
  `.option("maxStringLen", ...)` ([system/storage.md](../system/storage.md)).
- **Times inside the payloads are local with an offset**, and the offset changes with daylight
  saving: `+02:00` until 2026-10-25, `+01:00` after. Parse the offset, never assume one. Ours
  (`received_at`) are UTC.
- Most polls of weather and nearly all of the location tables return the same snapshot again.

## Does it line up with transit?

One real hour (2026-10-26, 08 UTC): every vehicle in Entur's vehicle positions (3,332, not
Ruter) against the 475 weather stations and the 3,672 situation points of that day.

| Mode | Vehicles | Nearest weather station (median, p90) | Station within 10 km | Situation point within 0.5 km / 2 km |
| ---- | -------: | ------------------------------------- | -------------------: | -----------------------------------: |
| bus | 2,941 | 5.5 km, 16.5 km | 73 % | 13 % / 47 % |
| rail | 118 | 4.2 km, 12.2 km | 81 % | 35 % / 68 % |
| ferry | 184 | 14.1 km, 33.4 km | 34 % | 2 % / 10 % |
| coach | 87 | 10.6 km, 312 km | 49 % | 21 % / 32 % |

- **Road weather lines up for buses and rail**, not for ferries: road stations are inland.
  Ferries vs wind needs coastal stations (Frost, [external/sources.md](../external/sources.md)).
- **Road situations are near where buses drive**: half the buses are within 2 km of one.
  Whether they *cause* delay is the question Gold #4 answers, not something to assume: most
  are long-running road works and lane management.
- Oslo (Ruter) has no vehicle positions; there, only NSR quay coordinates can place stops.

## For Silver (not decided, [pipeline/silver.md](../pipeline/silver.md) section 2)

- Grains: situation record version `(situationRecord id, version)`; road weather measurement
  `(site, measurementTimeDefault, index)`; travel time `(section, period start)`.
- Drop repeated snapshots by `_bronze_payload_sha256` **before** parsing XML.
- Reference tables from the location feeds, valid from–to: `weather_site` (point),
  `road_section` (polyline, projected to WGS84).
- Join to transit through a small mapping table (quay → nearest station or road section),
  built from coordinates. The join must not change row counts.
