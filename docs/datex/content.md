# DATEX: what the payloads contain

This page explains payload structure, joins and quality traps in NPRA's DATEX II 3 publications. Evidence comes
from snapshots on 2026-10-23 and 2026-10-26 and every poll of 2026-11-05.

Up: [DATEX access](sources.md) · Related: [Silver](../pipeline/silver.md),
[external sources](../external/sources.md)

## Structure

Each payload is one `ns2:messageContainer` using namespaces `http://datex2.eu/schema/3/...`.
Its single `payload` carries `publicationTime`.

| Feed | Payload |
| ---- | ------- |
| situation | ~1,150 `situation`, ~2,500 `situationRecord` total |
| traveltime | 404 `physicalQuantity`, two per section |
| weather | 475 `siteMeasurements`, ~3,770 `physicalQuantity` total |
| traveltime-locations | 202 `predefinedLocationReference`, id/name/polyline |
| weather-sites | one `measurementSiteTable`, 475 `measurementSite`, ~11 measurement types each |

Situations carry `situationVersionTime` and `overallSeverity`.
Travel-time quantities pair `TravelTimeData` for a 5-minute period with `TrafficStatus`.
Weather measurements carry `measurementSiteReference` and end with
`measurementTimeDefault/timeValue`. Site references hold name, point, road number and county.

## How the feeds link (verified 2026-10-26)

| Measurement reference | Target | Match |
| --------------------- | ------ | ----- |
| Travel-time `pertinentLocation/predefinedLocationReference/@id`, e.g. `100357` | location-table `predefinedLocationReference/@id` | all 202 |
| Weather `measurementSiteReference/@id`, e.g. `1629006` | `measurementSite/@id` | all 475 |
| Weather `physicalQuantity/@index` | site's `measurementSpecificCharacteristics/@index` | per site |

Example weather indices are 101 temperature, 201 humidity and 701 precipitation.

Travel time supplies `travelTime` and `freeFlowTravelTime` in seconds, e.g. 123 s versus
150 s. Their ratio measures congestion. Other fields give trend and status
(`freeFlow`, `slow`, `queuing`, `stationary`, `unknown`).
A section name is "Åsanevegen - Eikås", Bergen.

Weather site "Fv 714 Våvatnet" illustrates station naming. WGS84 coordinates span
58.1–71.2 °N and 5.0–31.0 °E, covering Norway.

## Situation records

A situation has a unique id, e.g. `NPRA_HBT_30-08-2021.06`.
Records have id, `version` and `xsi:type`.

| Type seen on 2026-10-26 | Records |
| ---------------------- | ------: |
| `MaintenanceWorks` | 1,170 |
| `RoadOrCarriagewayOrLaneManagement` | 609 |
| `GeneralNetworkManagement` | 504 |
| `SpeedManagement` | 225 |
| `ReroutingManagement` | 152 |

The tail includes obstructions, animals and public events.
No `Accident` appeared then, but 51 appeared during 2026-11-05.
The notes give no snapshot time for the 2,408-record checks below, so their total cannot be
reconciled with this type table.

**`(id, version)` is not unique.** In one snapshot, 36 of 2,408 records repeat it for
different measures at one place. Examples pair `temporaryTrafficLights` with
`trafficBeingManuallyDirected`, or `roadClosed` with emergency access.
Version times differ by milliseconds, so include `situationRecordVersionTime` in the key.
NPRA recommends that field for selecting the latest version.

Children ending in `Type` specify measures, e.g.
`roadOrCarriagewayOrLaneManagementType=roadClosed`. Silver retains `record_subtypes`.

### Validity and locations

`overallStartTime`..`overallEndTime` bounds validity. A passed end means expired.
No end means "until further notice".
Of 2,408 records, 1,014 (42 %) have recurring windows in `validPeriod`, using
`recurringTimePeriodOfDay` and `applicableDay`. June–December works might apply only
20:00–06:00, Monday–Thursday. Ignoring this wrongly puts night works in rush hour.

Recurring clocks carry today's offset, e.g. November's `+01:00` even alongside dates with
`+02:00`. Treat them as Oslo wall clock. `situationRecordCreationTime` and `overallStartTime` establish timing before
our first sighting. The oldest creation date seen was 2023-08-09.

Locations contain 3,672 WGS84 points and 2,668 `gmlLineString` lines in
`LocationGroupByList`. `SingleRoadLinearLocation` items duplicate Alert-C locations
without coordinates. Display points lie along lines, median 11 m from a vertex, rather than
at the start.

## When things appear: what the gaps mean

All feeds covered all 24 hours on Thursday 2026-11-05 without fetch errors.

| Feed | Observation |
| ---- | ----------- |
| situation | ~2,350 records every 5 min |
| situation | new versions/hour, 8–11 at night and 49–76 during 08–16 local |
| traveltime | 202 sections every 5 min, measured on 26–43 % at night and 60–65 % by day |
| traveltime | 55 of 202 sections never measured, including Asker-Holmen and Kløfta-Jessheim |
| traveltime | 65 sections below 10 % measurement coverage |
| weather | 475 stations, new measurements every 10 min |
| weather | 19 stations never updated, oldest listed measurement 1.6 years old |
| location tables | four polls/day, 20 of 24 hours absent, Bronze `missing_hours: 20` |
| location tables | travel-time hashes change only through `publicationTime`, sites unchanged 1–2 days |

Road works are mostly registered during working hours. Accidents and obstructions can
appear anytime. Travel-time status is `unknown` exactly when no time was measured.
Null means no measurement, not no traffic.
Half the travel-time and weather polls repeat because collection runs twice as often as
publication ([cadences](sources.md#feeds)). Frozen weather is flagged `stale_measurement` and
its station `usable=false`. Reference-table gaps are expected.

### Failed polls

All feeds had a 10-minute gap on 2026-10-31 and a 20-minute gap on 2026-11-03.
Bronze retains failures as `fetch_error`.

A valid XML response can instead report `exchangeInformation/.../returnStatus` with
`_extendedValue="fail"`, reason "Delivery break", without a payload.
This occurred once in ~12,000 polls over 2026-10-28 to 2026-11-08, weather at
2026-11-03 19:21:36 UTC. Silver quarantines `source_delivery_failed` without blocking the day.

### Sensor values to distrust (2026-11-05, flagged in Silver, never changed)

| Quantity | Observed | Interpretation |
| -------- | -------- | -------------- |
| air temperature | -39.9 °C with surface above zero | sensor failure, `suspect_air_temperature` for difference over 30 °C |
| humidity | 0 % to 103 % | zero indicates failure, ~103 is normal for these sensors |
| dew point | -81.9 °C | follows humidity zero |
| precipitation | constant 80.3 mm/h at three stations all day | `stuck_quantities` |
| visibility | constant 20,000 m at 164 of 303 stations | upper limit, not stuck |
| wind speed | median 2.6, p99 19.7, max 57.4 | resembles m/s, unit **UNVERIFIED**, DATEX specifies km/h |
| water film, ice, snow depth (`distance`) | mostly 0, p99 1.2, max 80 | unit **UNVERIFIED** |

## Pitfalls

- Three coordinate systems occur. Points use WGS84. Travel-time lines use EPSG:32633,
  UTM 33N metres. Situation lines use EPSG:25833, ETRS89 / UTM 33N.
- `pyproj` is not installed on the VM. [`silver_datex_parse.py`](../../pipeline/silver_datex_parse.py)
  converts UTM lines before spatial joins.
- Spark 3.5's Jackson 2.15 caps strings at 20,000,000 characters. Situation lines reach
  ~28.6M. Both Bronze readers raise `maxStringLen` ([storage](../system/storage.md#memory)).
- Payload timestamps include local offsets, changing from `+02:00` to `+01:00` at the
  2026-10-25 clock change. Parse offsets. Our `received_at` is UTC.
- Repeated snapshots are common. On 2026-11-05, half the weather polls repeated.
  Nearly all reference-table polls repeat across days.

## Does it line up with transit?

All 3,332 Entur vehicles at 2026-10-26 08 UTC, excluding Ruter, were compared with
475 weather stations and 3,672 situation points.

| Mode | Vehicles | Weather distance, median / p90 | Within 10 km | Situation within 0.5 / 2 km |
| ---- | -------: | ----------------------------: | -----------: | --------------------------: |
| bus | 2,941 | 5.5 / 16.5 km | 73 % | 13 % / 47 % |
| rail | 118 | 4.2 / 12.2 km | 81 % | 35 % / 68 % |
| ferry | 184 | 14.1 / 33.4 km | 34 % | 2 % / 10 % |
| coach | 87 | 10.6 / 312 km | 49 % | 21 % / 32 % |

Road weather aligns with buses and rail. Ferries need coastal stations such as
[Frost](../external/sources.md#overview). Nearly half the buses were within 2 km of a situation,
mostly long-running works or lane management. Proximity alone proves no delay effect.
That remains Gold product 4's question ([Gold](../pipeline/gold.md#4-road-incident-impact)).
Ruter has no vehicle positions, so Oslo stop placement uses NSR quays.

## In Silver

[`silver_datex.py`](../../pipeline/silver_datex.py) deduplicates record versions and
measurements, expands recurring validity into UTC-day intervals and flags suspect values.
It builds situation, travel-time, weather and reference tables plus quay mappings.
Unkeyed records and malformed payloads go to quarantine.
Grains, keys and spatial joins belong in [Silver's road tables](../pipeline/silver.md#roads).
See [reading safely](../pipeline/silver.md#reading-safely) for readiness rules.
