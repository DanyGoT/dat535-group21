# DATEX: what the payloads contain

The structure of NPRA's DATEX II 3 publications as we receive them, how they link together,
the traps, and what Silver should do with them. Access, feeds and sizes:
[datex/sources.md](sources.md). From real snapshots of every feed, 2026-10-23 and 2026-10-26, and
every poll of one full day, 2026-11-05.

## Structure

Every payload is one `ns2:messageContainer` (namespaces `http://datex2.eu/schema/3/...`) with
one `payload` element that carries its own `publicationTime`.

| Feed | Inside the payload |
| ---- | ------------------ |
| situation | ~1,150 `situation`, each with `situationVersionTime`, `overallSeverity` and one or more `situationRecord` (~2,500 in total) |
| traveltime | 404 `physicalQuantity`: two per road section, one `TravelTimeData` for a 5-minute period and one `TrafficStatus` |
| weather | 475 `siteMeasurements` (one per station: `measurementSiteReference`, ~3,770 `physicalQuantity` in all, then `measurementTimeDefault/timeValue` as the last child) |
| traveltime-locations | 202 `predefinedLocationReference` (id, name, polyline) |
| weather-sites | one `measurementSiteTable` with 475 `measurementSite` (id, name, point, road number, county, ~11 measurement types each) |

## How the feeds link (verified 2026-10-26)

| Measurement | Points to | Match |
| ----------- | --------- | ----- |
| traveltime `pertinentLocation/predefinedLocationReference/@id` (e.g. `100357`) | traveltime-locations `predefinedLocationReference/@id` | all 202 ids found |
| weather `measurementSiteReference/@id` (e.g. `1629006`) | weather-sites `measurementSite/@id` | all 475 ids found |
| weather `physicalQuantity/@index` (e.g. `101` temperature, `201` humidity, `701` precipitation) | the site's `measurementSpecificCharacteristics/@index` | per site |

- **Travel time** gives `travelTime` and `freeFlowTravelTime` in seconds for each section and
  5-minute period (e.g. 123 s against 150 s), plus a trend and a status (`freeFlow`, `slow`,
  `queuing`, `stationary`, `unknown`). Their ratio is a ready-made congestion index. Sections
  have names like "Åsanevegen - Eikås" (Bergen).
- **Weather sites** have a name ("Fv 714 Våvatnet"), a road number, a county, and a point in
  WGS84 latitude/longitude (58.1–71.2 °N, 5.0–31.0 °E: all of Norway).

## Situation records

- Each `situation` has a unique id (e.g. `NPRA_HBT_30-08-2021.06`); each `situationRecord` an
  id with a `version`, and a type (`xsi:type`). Seen on 2026-10-26: `MaintenanceWorks`
  1,170, `RoadOrCarriagewayOrLaneManagement` 609, `GeneralNetworkManagement` 504,
  `SpeedManagement` 225, `ReroutingManagement` 152, then a long tail (obstructions, animals,
  public events). No `Accident` at that moment, but 51 during 2026-11-05: they are short-lived.
- **`(id, version)` is not unique.** 36 of 2,408 records in a snapshot repeat it within their
  situation for a second measure at the same place (`temporaryTrafficLights` and
  `trafficBeingManuallyDirected`, `roadClosed` and open for emergency vehicles only). Their
  `situationRecordVersionTime` differs by milliseconds, so `(id, version, version time)` is the key.
- The concrete measure is in a child element ending in `Type`, e.g.
  `roadOrCarriagewayOrLaneManagementType` = `roadClosed`. Silver keeps them as `record_subtypes`.
- To pick the latest version, NPRA recommends `situationRecordVersionTime`.
- A record whose `overallEndTime` has passed is expired; no `overallEndTime` means "until
  further notice".
- **`overallStartTime`..`overallEndTime` only bounds a record.** 1,014 of 2,408 records (42 %)
  apply in recurring windows: `validPeriod` with `recurringTimePeriodOfDay` and `applicableDay`,
  e.g. road works from June to December but only 20:00-06:00, Monday to Thursday. Treating them as
  continuously active would put night works in the morning rush. The times of day always carry
  today's offset (`+01:00` in November, also on records whose dates say `+02:00`), so they are
  Oslo wall clock. Silver expands them into `road_situation_interval`.
- Records carry `situationRecordCreationTime` and `overallStartTime`, so we know when a
  situation started even if we first saw it later.
- Long-running road works dominate: the oldest record seen was created 2023-08-09.
- Locations: 3,672 WGS84 points (`latitude`/`longitude`) and 2,668 `gmlLineString` lines, in a
  `LocationGroupByList`. Its `SingleRoadLinearLocation` items are Alert-C duplicates without
  coordinates. The display point is on the line (median 11 m from a vertex), not at its start.

## When things appear: what the gaps mean

Every poll of 2026-11-05, a Thursday with no fetch errors. All feeds cover all 24 hours, but
what they contain is not evenly spread, and most of that is the source, not a fault of ours.

| Feed | Pattern | Why | Expected? |
| ---- | ------- | --- | --------- |
| situation | a full snapshot of ~2,350 records every 5 min, all day; but new record versions: 8-11 an hour at night, 49-76 from 08 to 16 local | operators register most road works in working hours. Accidents and obstructions come at any time | yes |
| situation | 42 % of records apply only in recurring windows (above) | that is what the record says | yes; use the intervals |
| traveltime | 202 sections every 5 min, all day; a travel time for 26-43 % of them at night, 60-65 % by day. Status is `unknown` exactly when there is none | no vehicles were measured. **55 of 202 sections reported nothing all day** (e.g. Asker-Holmen, Kløfta-Jessheim), 65 are below 10 % | yes: a null is "no measurement", not "no traffic" |
| traveltime | half of the polls repeat the last snapshot | we poll at 2.5 min, NPRA publishes every 5 | yes |
| weather | 475 stations, a new measurement every 10 min, half of the polls repeat | as above, 5 and 10 min | yes |
| weather | 19 stations did not update at all; the oldest measurement still listed was 1.6 years old | a dead station stays in the feed with its last values | source fault: flagged `stale_measurement`, station `usable=false` |
| location tables | four polls a day, so 20 of 24 hours have no file: `missing_hours: 20` in the Bronze record | our 6-hour poll | yes |
| location tables | `traveltime-locations` hashes differently on every poll | only `publicationTime` changes; the sites table stayed the same for 1-2 days | yes |

Failed polls are rare and short: a 10-minute gap on 2026-10-31, a 20-minute one on 2026-11-03,
every feed at once. They stay in Bronze as `fetch_error`.

### Sensor values to distrust (2026-11-05, flagged in Silver, never changed)

| Quantity | Seen | Reading |
| -------- | ---- | ------- |
| air temperature | -39.9 °C at sites whose road surface was above zero | failed sensor: `suspect_air_temperature` when air and surface differ by over 30 °C |
| relative humidity | 0 %, up to 103 % | 0 is failed, up to ~103 is normal for these sensors |
| dew point | -81.9 °C | follows from humidity 0 |
| precipitation | a constant 80.3 mm/h at three stations all day | stuck: station's `stuck_quantities` |
| visibility | constant 20,000 m at 164 of 303 stations | the sensors' upper limit, not stuck |
| wind speed | median 2.6, p99 19.7, max 57.4 | looks like m/s; the unit is **UNVERIFIED** (DATEX specifies km/h) |
| water film, ice, snow depth (`distance`) | mostly 0, p99 1.2, max 80 | unit **UNVERIFIED** |

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

## In Silver

Implemented in `pipeline/silver_datex.py`; tables and semantics in
[pipeline/silver.md](../pipeline/silver.md).
