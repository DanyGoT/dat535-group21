# Entur data sources

This page covers access, refresh patterns and usage rules for Entur's transport data. Read this before fetching
anything.

Up: [overview](../report/overview.md) · Related: [real-time feeds](realtime.md),
[identifiers](identifiers.md), [architecture](../system/architecture.md)

## Catalog

| Source | Content | Access | Use |
| ------ | ------- | ------ | --- |
| SIRI ET | times, cancellations, extra journeys | pub/sub push | collected since 2026-10-15 |
| SIRI SX | disruptions | pub/sub push | collected since 2026-10-15 |
| Vehicle positions | location, delay, occupancy | GraphQL websocket | collected since 2026-10-17 |
| NeTEx | journeys, lines, operators, stop sequences | daily zip | archived since 2026-10-26 |
| NSR | quays, stop places, coordinates | daily zip | archived since 2026-10-26 |
| BigQuery SIRI-ET | processed ET since 2020 | SQL in own GCP project | reference only |
| GTFS / GTFS-RT | derived NeTEx / SIRI subsets | download / feed | unused, partial coverage |

Portal: https://developer.entur.no/. Pages have Markdown versions, e.g.
`https://developer.entur.no/open-data/realtime.md`. GitHub: https://github.com/entur.
Research began 2026-10-15. Dates below identify checks.

## Usage rules

- Send **`ET-Client-Name`** on every request. Use `<org>-<application>`, lowercase, without
  spaces. Ours is `uis-dat535group21-collector`.
- Faked values are prohibited. Requests without the header get lower limits
  (https://developer.entur.no/terms-of-service.md).
- Open APIs need no authentication or key.
- [NLOD 2.0](https://data.norge.no/nlod/en/2.0) permits commercial reuse. Name Entur, link the licence, identify modifications and
  imply no endorsement. Credit text follows.

> Inneholder data under Norsk lisens for offentlige data (NLOD) tilgjengeliggjort av Entur AS

| API | Limit |
| --- | ----- |
| SIRI Lite | 4 requests/min |
| NeTEx, NSR | one download per 24 h, or the IP may be blocked |
| Journey Planner | 500 trip requests and 1000 other requests/min |

## Why archive timetables and stops?

ET alone gives delay where it reports aimed and actual times. NeTEx supplies the planned
denominator and attributes. NSR places stops on a map.

| Missing reference | What ET lacks | Measurement |
| ----------------- | ------------- | ----------- |
| NSR | coordinates | 0 `Latitude` in 176,630 calls, 2026-10-26 08 UTC |
| NeTEx | journeys never reported in ET | 0–8 % of planned journeys, VYG 8 % |
| NeTEx | attributes | `VehicleMode` 34 %, `PublishedLineName` 25 %, `OperatorRef` 64 % in one ET hour |

The missing-journey check covered departures before 14:00 local on 2026-10-23. Of VYG's
60 unseen trains, 36 were planned cancellations or replacements. These gaps risk bias in
ET-only rates. See [Silver's missing-journey check](../pipeline/silver.md#planned-journeys-unseen-in-et-2026-10-23)
for the sample and join evidence.
Vehicle positions supply coordinates, but Ruter publishes none.

NeTEx was 280 MB and NSR 41 MB on 2026-10-26, ~4 % of ET's daily volume.
[`static.py`](../../collector/static.py) archives them at 04:15 UTC using
[`dat535-static.timer`](../../collector/systemd/dat535-static.timer). It skips existing
daily files and downloads less than 20 h apart. **That guard does not enforce a rolling 24 h
limit.** On 2026-11-11, each archive had 15 day folders across 2026-10-26 to 2026-11-11,
with two days missing.

**Never download these archives by hand from the VM.** Read `landing/netex/` and
`landing/nsr/`. [Identifiers](identifiers.md#time) explains archive selection.

## Timetable (NeTEx, GTFS)

https://developer.entur.no/open-data/timetable

Nightly exports contain current and future plans and at most 48 h of past data. The
2026-10-23 export already lacked 2026-10-22 VYG and SKY journeys. Archive on the applicable day.
NeTEx's Nordic profile is the master. Derived GTFS loses detail.

| File | URL | Size |
| ---- | --- | ---: |
| National NeTEx | `https://storage.googleapis.com/marduk-production/outbound/netex/rb_norway-aggregated-netex.zip` | 280 MB, 2026-10-26 |
| Codespace NeTEx | `.../outbound/netex/rb_<codespace>-aggregated-netex.zip` | ~40 MB for SKY, e.g. `rb_sky-…`, 2026-10-22 |
| GTFS basic | `.../outbound/gtfs/rb_norway-aggregated-gtfs-basic.zip` | ~94 MB |
| GTFS extended | `.../outbound/gtfs/rb_norway-aggregated-gtfs.zip` | ~523 MB |

URLs were confirmed with `curl -I`, after discovery through the Mobility Database.
Entur's table renders client-side. Codespace files suit tests.
GTFS omits empty columns and unused stops, and rounds `shape_dist_traveled` to metres.

## Stops (NSR)

https://developer.entur.no/open-data/stops

Daily exports use `https://storage.googleapis.com/marduk-production/tiamat/`.
We use `Current_latest.zip`, current stops as NeTEx, 41 MB on 2026-10-26.
Alternatives include `Current_latest-gtfs.zip`, `CurrentAndFuture_latest.zip`,
`Full_latest.zip`, regional files such as `03_Oslo_latest.zip`, and `RailStations_latest.zip`.

## BigQuery history (reference)

Entur's processed SIRI-ET history is NLOD, refreshed daily, from Q1 2020.
Generated catalogs describe [last-recorded calls](../../entur-ai-docs/realtime_siri_et/README.md#quick-reference)
and [fixed-lead estimates](../../entur-ai-docs/estimated_siri_et/README.md#quick-reference).
See also https://data.entur.no/public/datasets. Do not edit the generated catalogs.

Queries require your own GCP project and an `operatingDate` filter.
The free sandbox allows 1 TB/month. No query has run, so access and contents remain
**UNVERIFIED**.

Both tables are under `ent-data-sharing-ext-prd.realtime_siri_et.`:

| Table | Content | Grain |
| ----- | ------- | ----- |
| `realtime_siri_et_last_recorded` | final aimed/actual times, cancellations, extra journeys/calls | `operatingDate, datedServiceJourneyId, sequenceNr` |
| `siri_et_estimated_times` | estimate versus actual at 1–45 min lead times | same key plus `timeDeltaMinutes` |

The catalogs inform Silver design. Same-day Silver/Gold cross-checks, prediction baselines
and possible training history remain planned ([departure-risk plan](../pipeline/gold.md#1-departure-risk-primary)).

BigQuery resembles our existing `stop_call`, but supplies processed fields rather than our
own Bronze/Silver work. It omits SX, vehicle positions and intermediate predictions outside
fixed lead times. No raw SIRI archive or bulk download exists. Our raw history starts when
collection starts. Old raw ET is therefore the least irreplaceable feed
([cleanup order](../system/storage.md#when-it-fills-delete-by-hand-cheapest-loss-first)).

## Useful repos

- `entur/anshar` implements pub/sub. See [subscription behaviour](realtime.md#pubsub-how-the-subscription-works).
- `entur/netex-java-model` contains NeTEx schema classes.
- `entur/tiamat` is the NSR backend.
