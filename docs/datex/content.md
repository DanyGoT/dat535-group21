# DATEX: what the payloads contain

The structure of NPRA's DATEX II 3 publications as we receive them, the traps, and what Silver
should do with them. Access, feeds and sizes: [datex/sources.md](sources.md). From one snapshot of each feed,
2026-10-23 ~13:36 UTC.

## Structure

Every payload is one `ns2:messageContainer` (namespaces `http://datex2.eu/schema/3/...`) with
one `payload` element that carries its own `publicationTime`.

| Feed | Inside the payload |
| ---- | ------------------ |
| situation | 1,148 `situation`, each with `situationVersionTime`, `overallSeverity`, `headerInformation` and one or more `situationRecord` (2,515 in total) |
| traveltime | 404 `physicalQuantity`, one per road section |
| weather | 475 `siteMeasurements` (one per station, with `measurementSiteReference` and `measurementTimeDefault`), 3,765 `physicalQuantity` in total |
| traveltime-locations | where the road sections are (structure **UNVERIFIED**) |
| weather-sites | where the stations are (structure **UNVERIFIED**) |

## Situation records

- Each `situation` has a unique id (e.g. `NPRA_HBT_30-08-2021.06`); each `situationRecord` a
  unique id with a `version`, and a type such as `MaintenanceWorks` or `Accident`
  (`xsi:type`).
- To pick the latest version, NPRA recommends `situationRecordVersionTime`.
- A record whose `overallEndTime` has passed is expired; no `overallEndTime` means "until
  further notice".
- Records carry `situationRecordCreationTime` and `overallStartTime`, so we know when a
  situation started even if we first saw it later.
- Long-running road works dominate: the oldest record in the snapshot was created 2023-08-09.

## Pitfalls

- **Spark's JSON reader caps strings at 20,000,000 characters** (Jackson 2.15 in Spark 3.5)
  and marks longer lines corrupt. A situation line is ~28.6M. Both Bronze jobs read with
  `.option("maxStringLen", ...)`.
- Times inside the payloads are local with offset (`2026-10-23T15:36:18.000+02:00`); ours
  (`received_at`) are UTC.
- Most polls of weather and nearly all of the location tables return the same snapshot again.

## For Silver (not decided)

- Grains: situation record version `(situationRecord id, version)`; road weather measurement
  `(site, measurementTimeDefault, quantity)`; travel time `(section, measurement time)`.
- Drop repeated snapshots by `_bronze_payload_sha256` **before** parsing XML.
- Join to transit through a small mapping table (quay → nearest station or road section),
  built from coordinates. **UNVERIFIED:** coordinates and road
  references in the location tables (not yet looked at).
