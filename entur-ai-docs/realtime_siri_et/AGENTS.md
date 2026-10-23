# AGENTS.md — Entur public data catalog

> Agent-facing index of every public BigQuery dataset published by `dataproduct-ror-insight`.

## How to use this catalog

Every public dataset lives under `agents/<schema>/<table>/` as a self-contained bundle:

| File | Purpose |
|------|---------|
| `README.md` | Human + agent prose: description, grain, refresh, columns, use cases, gotchas. **Start here.** |
| `dataset.jsonld` | schema.org/Dataset for machine consumers (Google Dataset Search, structured-data crawlers). |
| `examples.sql` | Hand-authored worked queries an agent can adapt directly. |
| `freshness.json` | Live `last_modified` and row count, refreshed at data-refresh cadence. |

A machine-readable catalog index covering every dataset is in [`manifest.json`](manifest.json).

## Access

All datasets in this catalog are **open** — queryable by any Google account with no access request.

### Set up (first time)
1. Have a GCP project (or use BigQuery's [free sandbox](https://cloud.google.com/bigquery/docs/sandbox), 1 TB/month).
2. Open the BigQuery console with **your** project selected in the top-left picker. Queries are billed to your project, not Entur's.
3. Optionally star `ent-data-sharing-ext-prd` to keep it pinned in the sidebar.

### Troubleshooting
- `Access denied` → wrong project selected. Switch the top-left picker to your own project.

### Partner / closed data
Not covered by this catalog. Partners get access via the Partner Portal and use service accounts created in Entur-Partner — see the [data.entur.no](https://data.entur.no/public/datasets) help page.

### Integration platforms
BigQuery has connectors for most platforms (PowerBI, Azure Data Factory, etc.). For ADF specifically, Entur supports Service Account auth via a Self-hosted Integration Runtime. See [BigQuery docs](https://cloud.google.com/bigquery/docs) for the full picture.

## Datasets

### nsr

| Dataset | Description | Grain | Refresh |
|---------|-------------|-------|---------|
| [`fare_zones_all_versions`](national_stop_registry/fare_zones_all_versions/README.md) | All historical versions of every Fare Zone in the Norwegian National Stop Registry (NSR), with polygon geometry. One row per (`id`, `version`). | id, version | daily |
| [`quays_all_versions`](national_stop_registry/quays_all_versions/README.md) | All historical versions of every Quay in the Norwegian National Stop Registry (NSR). | id, version | daily |
| [`quays_last_version`](national_stop_registry/quays_last_version/README.md) | Latest version of each Quay in the Norwegian National Stop Registry (NSR), including inactive ones. One row per Quay `id`. | id | daily |
| [`stop_places_all_versions`](national_stop_registry/stop_places_all_versions/README.md) | All historical versions of every Stop Place in the Norwegian National Stop Registry (NSR). One row per (`id`, `version`); use `validBetween` to determine which version was active at a given time. | id, version | daily |
| [`stop_places_last_version`](national_stop_registry/stop_places_last_version/README.md) | Latest version of each Stop Place in the Norwegian National Stop Registry (NSR), including inactive ones. One row per Stop Place `id`. Referenced in f.e quays. | id | daily |
| [`tariff_zones_all_versions`](national_stop_registry/tariff_zones_all_versions/README.md) | All historical versions of every Tariff Zone in the Norwegian National Stop Registry (NSR), with polygon geometry. One row per (`id`, `version`). | id, version | daily |
| [`topographic_places_all_versions`](national_stop_registry/topographic_places_all_versions/README.md) | All historical versions of every Topographic Place (administrative area: country, county, municipality, etc.) in the Norwegian National Stop Registry (NSR), with polygon geometry. | id, version | daily |

### siri_et

| Dataset | Description | Grain | Refresh |
|---------|-------------|-------|---------|
| [`realtime_siri_et_last_recorded`](realtime_siri_et/realtime_siri_et_last_recorded/README.md) | Recorded SIRI-ET data: the last message of a journey, providing a "trip summary". The most-used fields, with one row per stop in a journey. | operatingDate, datedServiceJourneyId, sequenceNr | daily |
| [`siri_et_estimated_times`](realtime_siri_et/siri_et_estimated_times/README.md) | Per-stop SIRI-ET estimated arrival/departure times compared against the recorded actuals at fixed lead times before the stop (e.g. the estimate 5 minutes before the actual arrival). | operatingDate, datedServiceJourneyId, sequenceNr, timeDeltaMinutes | daily |

## Conventions

- All public tables carry the BigQuery label `public-data-catalog: "true"`.
- Domain is set via the `domain` label; PII status via the `pii` label.
- Grain is declared via `uniqueKey` in the `.sqlx` config.
- Refresh cadence is encoded as a tag (`daily`, `hourly`, …).
- Owner contact lives in the `slack` and `team` labels.

## Generation

This file and everything under `agents/` is auto-generated from Dataform configs and co-located sidecars. Do not edit by hand.
