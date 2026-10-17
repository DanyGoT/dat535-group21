# Entur data sources

Researched 2026-10-15. Main portal: https://developer.entur.no/ (pages can be fetched as `.md`,
e.g. `https://developer.entur.no/open-data/realtime.md`). GitHub: https://github.com/entur.

## Usage rules

- **`ET-Client-Name` header is required on every request** to every Entur API. Format:
  `<org>-<application>`, lowercase, no spaces. Faked values are prohibited; missing header
  gets lower limits. (https://developer.entur.no/terms-of-service.md)
- No authentication or API key for open feeds.
- Limits:
  - SIRI Lite (GET `/rest/*`): 4 requests/min.
  - Static dumps: max one download per 24 h, or your IP may be blocked.
  - Journey Planner: 500 trip / 1000 other requests per min with the header.
- **Licence:** NLOD 2.0 (reuse incl. commercial allowed). Attribution required: name Entur,
  link the licence, state if modified, don't imply endorsement. Default text:
  *"Inneholder data under Norsk lisens for offentlige data (NLOD) tilgjengeliggjort av Entur AS"*.

## Scheduled data (timetables)

Source: https://developer.entur.no/open-data/timetable

- Re-validated and exported **nightly**. Holds the current and future plan with **at most
  48 h of past data** — no history. We must archive our own daily copy.
- NeTEx (Nordic profile) is the most complete; GTFS is a derived subset ("extended" and
  "basic"). Prefer NeTEx as the join backbone.

| Dataset | URL | Size (2026-10-15) |
| ------- | --- | ----------------- |
| NeTEx national | `https://storage.googleapis.com/marduk-production/outbound/netex/rb_norway-aggregated-netex.zip` | ~275 MB |
| GTFS basic | `https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_norway-aggregated-gtfs-basic.zip` | ~94 MB |
| GTFS extended | `https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_norway-aggregated-gtfs.zip` | ~523 MB |

- These URLs were confirmed live with `curl -I`. They were found via Mobility Database, not
  Entur's own docs (the docs table renders client-side).
- Per-operator files exist, but their filenames are **UNVERIFIED**.
- GTFS export quirks: empty columns omitted, `shape_dist_traveled` rounded to metres,
  unused stops not exported.

## Stops (National Stop Register, NSR)

Source: https://developer.entur.no/open-data/stops — NLOD, updated daily.
Base: `https://storage.googleapis.com/marduk-production/tiamat/`

- `Current_latest.zip` — NeTEx, current stops (use this).
- Others: `Current_latest-gtfs.zip`, `CurrentAndFuture_latest.zip`, `Full_latest.zip`,
  regional (e.g. `03_Oslo_latest.zip`), `RailStations_latest.zip`.

## Real-time data

See `realtime.md`. Use SIRI ET (delays/cancellations) and optionally SIRI SX (alerts).
Skip GTFS-RT (no national feed, partial coverage) and SIRI VM.

Vehicle positions are collected by `collector/vehicles.py` from the
Vehicle Positions GraphQL API v2 (https://developer.entur.no/apis/graphql/vehicle-positions):

- Subscriptions: `wss://api.entur.io/realtime/v2/vehicles/subscriptions`, subprotocol
  `graphql-transport-ws` (verified 2026-10-17; the docs say "graphql-ws"). Queries:
  `POST https://api.entur.io/realtime/v2/vehicles/graphql`. `ET-Client-Name` required.
- `subscription { vehicles(...) }` with no filter = all of Norway. Server batches updates
  (`bufferSize` default 20, `bufferTime` default 250 ms). A new subscription starts with a
  snapshot of all current vehicles.
- Introspection works per type (`__type(name: ...)`); a full `__schema` dump is refused.
- Field coverage in one minute of data (2026-10-17): location, delay, line, serviceJourney,
  occupancyStatus ~100 %; monitoredCall 92 %; progressBetweenStops 69 %; datedServiceJourney
  44 %; speed 10 %. Mostly BUS (91 %), then FERRY, COACH, RAIL, TRAM.
- First closed hour (2026-10-17 12:20–13:00 UTC, Saturday): ~150 vehicle updates/s from 1,706
  vehicles in 22 codespaces, 0.2 % exact repeats (same vehicle and `lastUpdated`), received
  5 s after `lastUpdated` at the median (p99 24 s). 39 min = 369 MB raw, 39 MB gzipped.
- **Ruter (RUT) publishes no positions here**: `vehicles(codespaceId: "RUT")` is empty although
  RUT is listed in `codespaces`. Oslo is covered by ET only.
- Rate limits and history: not documented. **UNVERIFIED**.

## Historical real-time

No public archive found (no historical SIRI, BigQuery or bulk download). **UNVERIFIED** —
the Entur Confluence space wasn't fully searched; asking Entur would settle it. Until then,
analysis depth = how long we have been collecting.

## Relevant repos

- `entur/anshar` — Entur's SIRI pub/sub hub (reference for subscription behaviour).
- `entur/netex-java-model` — NeTEx schema classes (reference for structure).
- `entur/tiamat` — NSR backend.
