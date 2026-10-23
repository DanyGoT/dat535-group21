# siri_et_estimated_times

> Per-stop SIRI-ET estimated arrival/departure times compared against the recorded actuals at fixed lead times before the stop (e.g. the estimate 5 minutes before the actual arrival).

## Quick reference

| | |
|---|---|
| **BigQuery table** | `ent-data-sharing-ext-prd.realtime_siri_et.siri_et_estimated_times` |
| **Domain** | `siri_et` |
| **Grain** | One row per `operatingDate` + `datedServiceJourneyId` + `sequenceNr` + `timeDeltaMinutes` |
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

Refreshed daily. Per-stop SIRI-ET estimated arrival/departure times compared against the recorded actuals at fixed lead times before the stop (e.g. the estimate 5 minutes before the actual arrival). Lets you assess the quality of real-time estimates and the journey information available to travellers. Based on Entur's SIRI-ET stream covering multiple Norwegian operators — see developer.entur.org/pages-real-time-intro.

## Columns

| Column | Description |
|--------|-------------|
| `operatingDate` | Date of the vehicle journey. For example: 2020-01-10 |
| `dataSource` | Codespace of the data source. For example: AKT (Agder kollektivtrafikk) |
| `transportMode` | Transport mode for the vehicle Journey. For example: BUS |
| `lineRef` | Reference to the line of the vehicle journey. For example: AKT:Line:100 |
| `serviceJourneyId` | ID of the service journey. For example: RUT:ServiceJourney:17-154797-20890925 |
| `datedServiceJourneyId` | ID of the service journey with date. For example: 2021-09-09:RUT:ServiceJourney:17-154797-20890925 |
| `stopPointRef` | Served quay by the vehicle journey in NSR format. For example: NSR:Quay:105273 (Storgata) |
| `parentStopPointRef` | Parent stop point for th equay specified in StopPointRef. |
| `stopPointName` | Name of stop. For example: Storgata |
| `sequenceNr` | Number in the sequence of served stops. For example: 23 |
| `timeDeltaMinutes` | Describes how many minutes before the actual arrival/departure the estimated times are taken from. Example: 10 |
| `AimedArrivalTime` | Planned arrival time of vehicle at a stop. For example: 2020-02-13 23:23:00 UTC |
| `arrivalTime` | Actual arrival time of a vehicle at a stop. For example: 2020-02-13 23:25:11 UTC |
| `estimatedArrivalTime` | Estimated arrival time of a vehicle at a stop at a certain point in time(timeDelataMinutes before arrivalTime). For example: 2020-02-13 23:23:10 UTC |
| `EstimatedArrivalRecordedAtTime` | Timestamp when estimatedArrivalTime was generated. Closest timestamp larger than arrivaltime - timeDeltaMinutes |
| `AimedDepartureTime` | Planned departure time of a vehicle from a stop. For example: 2020-02-13 23:23:00 UTC |
| `departureTime` | Actual departure time of a vehicle from a stop. For example: 2020-02-13 23:25:11 UTC |
| `estimatedDepartureTime` | Estimated departure time of a vehicle at a stop at a certain point in time(timeDelataMinutes before departureTime). For example: 2020-02-13 23:23:10 UTC |
| `EstimatedDepartureRecordedAtTime` | Timestamp when estimatedDepartureTime was generated. Closest timestamp larger than departuretime - timeDeltaMinutes |

> Column types come from BigQuery `INFORMATION_SCHEMA.COLUMNS` at portal render time, or from the freshness manifest. They are intentionally not embedded in this file.

## Use cases

- Measure real-time estimate accuracy: how far the estimated arrival/departure was from the recorded actual, at a fixed lead time before the stop.
- See how estimates converge as a vehicle approaches a stop by comparing rows across `timeDeltaMinutes` (1, 3, 5, 10, 15, 20, 30, 45).
- Benchmark estimate quality per operator (`dataSource`), line (`lineRef`) or `transportMode`.
- Find stops, lines or operators whose estimates are unreliable far in advance (large delta at high `timeDeltaMinutes`).
- Quantify the journey information available to travellers ahead of a departure.

## Related tables

| Table | Relation |
|-------|----------|
| `realtime_siri_et_last_recorded` | The recorded SIRI-ET actuals this table is derived from. Join on `datedServiceJourneyId` + `sequenceNr` for the underlying call. |
| `quays_last_version` | Resolve `stopPointRef` (a Quay id) to stop metadata — join `quays_last_version.id = stopPointRef`. |
| `stop_places_last_version` | Reach the parent Stop Place (name, location, transport mode) via `quays_last_version.stopPlaceRef`. |

## Gotchas

- Huge table (~9 billion rows) partitioned by `operatingDate`. Always filter on `operatingDate` — unfiltered scans are very expensive.
- `timeDeltaMinutes` is the lead time before the stop, so each call appears in multiple rows (one per measurement point). Filter to a single `timeDeltaMinutes` to get one row per stop.
- Estimate error = `estimatedArrivalTime` − `arrivalTime` (and the departure equivalents). `*RecordedAtTime` is when the estimate was observed, not the estimate itself.
- Each `estimatedArrivalTime` is the last estimate available BEFORE `timeDeltaMinutes`, to reflect the information a traveller had available at this specific point in time. If estimates are not updated frequently, the difference between when the estimate was generated and `timeDeltaMinutes` can be large.
- In Entur's own visualisation [Estimate Precision](https://datastudio.google.com/s/v5p8AJIxIiA), if `estimatedArrivalTime` is entirely missing at `timeDeltaMinutes`, the aimedArrivalTime is filled in to reflect that travellers only had schedule data available while tracking a journey.
- Several data sources start generating estimates only AFTER a journey has begun, so estimates for early stops in the sequence can be missing for larger values of `timeDeltaMinutes`.
- `stopPointRef` is a Quay id, not a Stop Place id. Join via `quays_last_version.stopPlaceRef` to reach a Stop Place.
- Column-name casing is mixed (e.g. `AimedArrivalTime` vs `arrivalTime`). Match the exact casing in the schema.

## Examples

See [`examples.sql`](examples.sql) for runnable queries.

## Machine-readable metadata

- Schema.org/Dataset: [`dataset.jsonld`](dataset.jsonld)
- Catalog entry: [`../../manifest.json`](../../manifest.json)
