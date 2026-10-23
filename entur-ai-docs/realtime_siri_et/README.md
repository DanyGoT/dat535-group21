# realtime_siri_et_last_recorded

> Recorded SIRI-ET data: the last message of a journey, providing a "trip summary". The most-used fields, with one row per stop in a journey.

## Quick reference

| | |
|---|---|
| **BigQuery table** | `ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded` |
| **Domain** | `siri_et` |
| **Grain** | One row per `operatingDate` + `datedServiceJourneyId` + `sequenceNr` |
| **Refresh** | Daily |
| **Contains PII** | No |
| **Owner** | Slack [#talk-data](https://entur.slack.com/archives/talk-data) · team `data` |
| **Live freshness** | See [`freshness.json`](freshness.json) |

## Access

Open dataset — queryable by any Google account, no request needed.

- Queries are billed to **your own GCP project**, not Entur's. Select your project in BigQuery's top-left picker, or you'll see `Access denied`.
- No GCP project yet? BigQuery's [free sandbox](https://cloud.google.com/bigquery/docs/sandbox) gives 1 TB of query processing per month.
- Star `ent-data-sharing-ext-prd` to pin it in your BigQuery sidebar.

Full access guide (including partner/closed data): [data.entur.no](https://data.entur.no/public/datasets).

## Description

Refreshed daily. Recorded SIRI-ET data: the last message of a journey, providing a "trip summary". The most-used fields, with one row per stop in a journey. Covers multiple Norwegian operators from Q1 2020 onward and is updated every morning. Based on Entur's SIRI-ET stream — see developer.entur.org/pages-real-time-intro.

## Columns

| Column | Description |
|--------|-------------|
| `recordedAtTime` | The time when the data object was created/published. For example: 2020-02-13 23:29:54.82 UTC |
| `lineRef` | Reference to the line of the vehicle journey. For example: AKT:Line:100 |
| `directionRef` | Reference to the relative direction of the vehicle journey. For example: 2 (Outbound) |
| `operatingDate` | Date of the vehicle journey. For example: 2020-01-10 |
| `dayOfTheWeek` | Day of the week. For example: 3 (Wednesday) |
| `serviceJourneyId` | ID of the service journey. For example: RUT:ServiceJourney:17-154797-20890925 |
| `datedServiceJourneyId` | ID of the service journey with date. For example: 2021-09-09:RUT:ServiceJourney:17-154797-20890925 |
| `operatorRef` | ID to the corresponding company responsible for vehicle journey. For example: nettbuss |
| `vehicleMode` | Transport type. For example: bus |
| `extraJourney` | Boolean indicator of if vehicle journey is a replacement departure. For example: true (vehicle journey is a replacement departure) |
| `journeyCancellation` | Boolean indicator of if vehicle journey is a cancelled departure. For example: true (vehicle journey is a cancelled departure) |
| `stopPointRef` | Served quay by the vehicle journey in NSR format. For example: NSR:Quay:105273 (Storgata) |
| `sequenceNr` | Number in the sequence of served stops. For example: 23 |
| `stopPointName` | Name of stop. For example: Storgata |
| `originName` | Name of the first stop of the departure. For example: Skøyen stasjon |
| `destinationName` | Name of the last stop of the vehicle journey. For example: Simensbråten |
| `extraCall` | Boolean indicator of whether the served stop is in addition to the planned stop sequence |
| `stopCancellation` | Boolean indicator for whether a stop is cancelled in this journey. For example: True (Stop is not served on this journey) |
| `estimated` | Boolean indicator of whether the data is estimated |
| `aimedArrivalTime` | Planned arrival time of vehicle at a stop. For example: 2020-02-13 23:23:00 UTC |
| `arrivalTime` | Actual arrival time of a vehicle at a stop. For example: 2020-02-13 23:25:11 UTC |
| `aimedDepartureTime` | Planned departure time of a vehicle from a stop. For example: 2020-02-13 23:23:00 UTC |
| `departureTime` | Actual departure time of a vehicle from a stop. For example: 2020-02-13 23:25:11 UTC |
| `dataSource` | Codespace of the data source. For example: AKT (Agder kollektivtrafikk) |
| `dataSourceName` | Name of the data source. For example: Agder kollektivtrafikk |

> Column types come from BigQuery `INFORMATION_SCHEMA.COLUMNS` at portal render time, or from the freshness manifest. They are intentionally not embedded in this file.

## Use cases

- Measure punctuality: compare actual (`arrivalTime` / `departureTime`) against planned (`aimedArrivalTime` / `aimedDepartureTime`) per stop.
- Track cancellations — whole journeys (`journeyCancellation`) or individual stops (`stopCancellation`) — by line, operator or stop.
- Reconstruct a vehicle journey's served stops in order (`datedServiceJourneyId` + `sequenceNr`).
- Analyse real-time data coverage and quality per operator (`dataSource`) or line (`lineRef`).
- Join recorded stop usage to the National Stop Registry for stop names, locations and transport modes.

## Related tables

| Table | Relation |
|-------|----------|
| `siri_et_estimated_times` | Estimate-quality companion derived from the same SIRI-ET stream (estimated vs recorded times at fixed lead times). Join on `datedServiceJourneyId` + `sequenceNr`. |
| `quays_last_version` | Resolve `stopPointRef` (a Quay id) to stop metadata — join `quays_last_version.id = stopPointRef`. |
| `stop_places_last_version` | Reach the parent Stop Place (name, location, transport mode) via `quays_last_version.stopPlaceRef`. |

## Gotchas

- Huge table (~2.9 billion rows) partitioned by `operatingDate`. Always filter on `operatingDate` — unfiltered scans are very expensive.
- One row per stop, not per journey. A single journey spans many rows; order them by `sequenceNr`.
- `stopPointRef` is a Quay id, not a Stop Place id. Join via `quays_last_version.stopPlaceRef` to reach a Stop Place.
- The combination of operatingDate and serviceJourney or datedServiceJourneyId describes a unique Journey.
- ALL train data had a common data source Bane NOR (BNR) in the past, and selecting a single train company has to be done via other identifiers (substring of serviceJourneyId or lineRef). Over time each train company switched to their own SIRI-ET stream.
- Data quality issues were widespread — especially in the past. For an overview of data quality per PTA over time, visit [data-quality.entur.no](https://data-quality.entur.no/SIRI_ET).
- Due to data quality issues, some operators fill in missing arrivalTime and departureTime with aimedArrivalTime and aimedDepartureTime. When calculating delays, these can pull down aggregate statistics.
- Delay = actual − aimed (`arrivalTime` − `aimedArrivalTime`). `recordedAtTime` is when the message was published, not the arrival/departure.
- Rows can be estimated rather than recorded (`estimated = true`), and stops/journeys can be cancelled (`stopCancellation` / `journeyCancellation`) — filter on these depending on the analysis.
- Column-name casing is mixed (e.g. `aimedArrivalTime` vs `arrivalTime`). Match the exact casing in the schema.

## Examples

See [`examples.sql`](examples.sql) for runnable queries.

## Machine-readable metadata

- Schema.org/Dataset: [`dataset.jsonld`](dataset.jsonld)
- Catalog entry: [`../../manifest.json`](../../manifest.json)
