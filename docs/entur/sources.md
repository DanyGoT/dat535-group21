# Entur data sources

Every Entur source we use or considered, how to get it, and the rules for using it. Portal:
https://developer.entur.no/ (any page can be fetched as `.md`, e.g.
`https://developer.entur.no/open-data/realtime.md`). GitHub: https://github.com/entur.
Researched from 2026-10-15; dates mark when a fact was checked.

## Catalog

| Source | Gives us | How | Status |
| ------ | -------- | --- | ------ |
| SIRI ET | real-time times, cancellations, extra journeys | pub/sub push | **collected** ([entur/realtime.md](realtime.md)) |
| SIRI SX | disruption messages | pub/sub push | **collected** |
| Vehicle positions | location, delay, occupancy per vehicle | GraphQL websocket | **collected** ([entur/realtime.md](realtime.md)) |
| NeTEx timetable | planned journeys, lines, operators, stop sequences | daily zip download | **not collected yet**; no history, every missed day is lost |
| NSR stop register | quays, stop places, coordinates | daily zip download | **not collected yet** |
| BigQuery SIRI-ET history | processed ET since 2020 | SQL (own GCP project) | reference only, not pipeline input |
| GTFS / GTFS-RT | derived subsets of NeTEx / SIRI | download / feed | not used: partial coverage |

## Do we need the plan (NeTEx, NSR) next to real-time?

The plan and the stop register are not where real-time comes from; they describe what *should*
happen and where stops *are*. ET alone is enough for delay at the stops it reports (it carries
aimed and actual times). What we miss without them, measured:

| Without | We miss | Evidence |
| ------- | ------- | -------- |
| NSR | **every coordinate**: ET has quay ids but no position, so nothing can be joined to weather, road events or a map | 0 `Latitude` in 176,630 ET calls (2026-10-26 08 UTC); vehicle positions have coordinates but not for Ruter |
| NeTEx | journeys that never appear in real-time, incl. cancellations planned in advance: punctuality and cancellation rates from ET alone are biased per operator | 0–8 % of planned journeys never seen in ET, VYG 8 % (36 of 60 planned cancellations) ([pipeline/silver.md](../pipeline/silver.md) §4) |
| NeTEx | line names, operators and modes for many journeys | in one ET hour: `VehicleMode` on 34 % of journeys, `PublishedLineName` 25 %, `OperatorRef` 64 % |

Cost: NeTEx 280 MB and NSR 41 MB a day (2026-10-26), ~4 % of ET's daily volume; one download
each per day is within Entur's limit. They can't be fetched later (≤ 48 h of past data). So:
worth archiving daily as raw landing data, read by Bronze like the rest.

## Is real-time redundant with the plan and BigQuery?

No. NeTEx, GTFS and NSR are the plan and the stops, not what happened. BigQuery's
`realtime_siri_et_last_recorded` is close to our future Silver `stop_call` table (final times
per call, back to 2020), but it is Entur's processed product: ET only (no SX, no vehicle
positions), no intermediate predictions except fixed lead times, refreshed once a day, and not
something we build ourselves (the course grades our Bronze and Silver). It is valuable as a
cross-check for our Silver, and possibly as years of extra training history for the departure
risk model ([pipeline/gold.md](../pipeline/gold.md) #1), if we decide to use it.

## Usage rules

- **`ET-Client-Name` on every request** to every Entur API: `<org>-<application>`, lowercase,
  no spaces; ours is `uis-dat535group21-collector`. Faked values are prohibited; without it
  limits are lower (https://developer.entur.no/terms-of-service.md).
- No authentication or API key for open data.
- Limits: SIRI Lite 4 requests/min; static dumps **one download per 24 h** (or the IP may be
  blocked); Journey Planner 500 trip / 1000 other requests per min.
- **Licence NLOD 2.0** (reuse, also commercial). Name Entur, link the licence, say if the data
  was modified, don't imply endorsement. Default text: *"Inneholder data under Norsk lisens for
  offentlige data (NLOD) tilgjengeliggjort av Entur AS"*.

## Timetable (NeTEx, GTFS)

https://developer.entur.no/open-data/timetable

- Exported nightly with the current and future plan and **at most 48 h of past data** (in
  practice less: the 2026-10-23 export no longer held 2026-10-22 journeys for VYG and SKY).
  We must archive our own copy **on the day** it applies.
- NeTEx (Nordic profile) is the master; GTFS is derived and loses detail. Join on NeTEx
  ([entur/identifiers.md](identifiers.md)).

| File | URL | Size |
| ---- | --- | ---: |
| NeTEx, all of Norway | `https://storage.googleapis.com/marduk-production/outbound/netex/rb_norway-aggregated-netex.zip` | 280 MB (2026-10-26) |
| NeTEx, one codespace | `.../outbound/netex/rb_<codespace>-aggregated-netex.zip` (e.g. `rb_sky-…`, 2026-10-22) | ~40 MB (SKY) |
| GTFS basic | `.../outbound/gtfs/rb_norway-aggregated-gtfs-basic.zip` | ~94 MB |
| GTFS extended | `.../outbound/gtfs/rb_norway-aggregated-gtfs.zip` | ~523 MB |

- URLs confirmed with `curl -I`; found via the Mobility Database (Entur's own table renders
  client-side). Per-codespace files are handy for tests.
- GTFS quirks: empty columns omitted, `shape_dist_traveled` rounded to metres, unused stops
  not exported.

## Stops (NSR)

https://developer.entur.no/open-data/stops, updated daily. Base:
`https://storage.googleapis.com/marduk-production/tiamat/`

- `Current_latest.zip`: current stops as NeTEx, 41 MB (2026-10-26). **Use this.**
- Also: `Current_latest-gtfs.zip`, `CurrentAndFuture_latest.zip`, `Full_latest.zip`, regional
  (`03_Oslo_latest.zip`), `RailStations_latest.zip`.

## BigQuery history (reference)

Processed SIRI-ET history as open BigQuery tables (NLOD, refreshed daily, from Q1 2020).
Catalog: `entur-ai-docs/` (generated by Entur, don't edit) and https://data.entur.no/public/datasets.
Queries need your own GCP project (the free sandbox gives 1 TB of queries/month) and must
filter on `operatingDate`.

| Table (`ent-data-sharing-ext-prd.realtime_siri_et.`) | Content | Grain |
| --------------------------------------------------- | ------- | ----- |
| `realtime_siri_et_last_recorded` | last message per journey: aimed and actual times, cancellations, extra journeys and calls | operatingDate, datedServiceJourneyId, sequenceNr |
| `siri_et_estimated_times` | estimate vs actual at fixed lead times (1–45 min) | + timeDeltaMinutes |

- Entur's own product: a subset of fields, ET only. We use it to design Silver, to cross-check
  our Silver and Gold numbers for the same day, and as a baseline for predictions ([pipeline/gold.md](../pipeline/gold.md)).
- It also means old raw ET is the least irreplaceable thing we store ([system/storage.md](../system/storage.md)).
- No query run yet: access and contents are **UNVERIFIED**.
- No raw SIRI archive or bulk download exists. Raw-data depth = how long we have collected.

## Useful repos

- `entur/anshar`: the SIRI pub/sub hub (subscription behaviour, [entur/realtime.md](realtime.md)).
- `entur/netex-java-model`: NeTEx schema classes.
- `entur/tiamat`: the NSR backend.
