# Silver

Silver turns Bronze (every message, as received) into trusted tables with a stated grain,
keys and quality flags, shared by every Gold product ([pipeline/gold.md](gold.md)). Nothing is built yet; the
rules below hold, the tables are a proposal. Keys and id quirks: [entur/identifiers.md](../entur/identifiers.md).

## Rules in brief

1. State the grain in one sentence before writing a transformation. One table, one grain.
2. Reduce real-time to one row per key **before** joining; a join must not change row counts.
3. Join each operating day to the newest plan archived on or before that day (`landing/netex/`).
4. Flag, never drop: a failed rule sets a `dq_issue`; quarantined rows stay countable.
5. Parse and project to narrow columns before any shuffle. A raw ET day is ~160 GB of text.
6. Same input, same output: process per operating day, overwrite that partition.

## 1. Grain first

| Grain | One row = | Key |
| ----- | --------- | --- |
| Stop call (fine) | one journey at one stop position on one operating day | `(DataFrameRef, DatedVehicleJourneyRef, Order)` |
| Journey (medium) | one journey on one operating day | `(DataFrameRef, DatedVehicleJourneyRef)` |
| Aggregate (coarse) | e.g. line × day | Gold |

A coarser table is built by aggregating a finer one, not by joining. Vehicle positions carry
`serviceJourney {id, date}` and `monitoredCall {order}`, so the same journey and stop keys fit
ET and vehicle positions. SX is different: a situation names the lines, stops or journeys it
affects and when it is valid, so it links to calls through those, never as a stop call.

## 2. Tables (proposed)

| Table | One row = | Key | From | Gold ([pipeline/gold.md](gold.md)) |
| ----- | --------- | --- | ---- | ---------------- |
| `stop_call` | final state of one call | operating date, journey id, order | ET (+ NeTEx plan) | 1–8 |
| `journey` | one journey on one operating day | operating date, journey id | ET + NeTEx | 1, 2, 7, 8 |
| `call_prediction` | latest predicted time of a call as known at a fixed lead time (1, 5, 10, 20, 30 min) before its planned or its actual time | stop call key, reference (planned/actual), lead time | ET versions | 6, 1 |
| `vehicle_position` | one position report | vehicle id, `lastUpdated` | vehicles | 3, 4, 9 |
| `sx_situation` | one version of one SX situation | situation number, version | SX | 7 |
| `road_situation` | one version of one DATEX situation record | record id, version | DATEX situation | 1, 4 |
| `road_weather` | one measurement | site, measurement time, quantity | DATEX weather | 1, 5 |
| `road_travel_time` | one road section's travel time | section, measurement time | DATEX travel time | 1, 3 |
| `quay`, `stop_place`, `line`, `weather_site`, `road_section` | one entity, valid from–to | id, valid from | NSR, NeTEx, DATEX tables | all |
| `quay_weather_site`, `quay_road_situation` | one link (per day for situations) | both ids (+ day) | computed (distance) | 1, 4, 5 |

Keep ids in facts and labels in reference tables; Gold adds the labels.

## 3. Join real-time to the plan on the same grain

- **Journey:** id **and** operating day; the id alone repeats every day it runs.
- **Stop:** position in the journey (`Order`), not the quay: 855 of 15,405 planned journeys
  visit a quay twice.
- **Normalise ids first:** NeTEx `DatedServiceJourney` → `ServiceJourneyRef` + `OperatingDayRef`
  turns VYG's ids into the normal key.

Measured: one hour (`siri-et/date=2026-10-22/hour=06`) against the national NeTEx, keyed on
`DataFrameRef` + `DatedVehicleJourneyRef` + `Order`; NeTEx side `StopPointInJourneyPattern/@order`,
quay via `ScheduledStopPoint` → `PassengerStopAssignment`.

| Outcome | Stop calls | Cause |
| ------- | ---------: | ----- |
| match on key and quay | 398,626 (97.9 %) | |
| journey not in plan | 2,654 | plan downloaded a day late: the 22nd was already gone (mostly SKY) |
| no `FramedVehicleJourneyRef` | 3,337 | VYG trains (`VYG:DatedServiceJourney:…`) and extra journeys (`EstimatedVehicleJourneyCode`) |
| same order, other quay | 1,728 | platform changes, or a mapping gap; needs a look |
| order not in plan | 721 | mostly FLT, from our single-pass parser (FLT files list journeys before stop points) |

**Delay alone needs no plan:** ET has aimed times. The plan adds what ET can't show: journeys
that never appear in real-time, and stop, line and operator attributes. Archive it the same
day; a day late is already too late.

## 4. Planned but never in real-time

Journeys planned for 2026-10-23, starting before 14:00 local, never seen in that day's ET
(8 per-codespace NeTEx files):

| | AKT | FLT | ATB | BRA | RUT | SKY | KOL | VYG |
| - | -: | -: | -: | -: | -: | -: | -: | -: |
| planned | 1992 | 103 | 3520 | 1466 | 9097 | 6394 | 3072 | 752 |
| never seen | 0 % | 0 % | 0.8 % | 0.8 % | 1.3 % | 3.0 % | 6.2 % | 8.0 % |

- VYG: 36 of its 60 unseen trains are `ServiceAlteration` = `cancellation`/`replaced` in
  NeTEx: cancellations planned in advance, which only the plan shows.
- The other way round: SKY reported 234 journeys in ET that are not in that day's NeTEx.
- So punctuality or cancellation rates from ET alone are biased per operator, by up to ~8 %.

## 5. Deduplicate

**SIRI ET** restates a journey on every change, and in full after each (re)subscribe. In the
hour above, 23.3 M call versions collapsed to 407 k stop calls, ~57 versions each. Keep the
latest state per key:

- order by `RecordedAtTime` (producer), then `received_at` (ours), then payload hash;
- an actual time (`RecordedCall`) beats any expected one.

**DATEX** polls return the full snapshot each time; repeated snapshots are byte-identical.
Drop repeats by `_bronze_payload_sha256` **before** parsing XML, then keep one row per
`(situationRecord id, version)`, latest `situationRecordVersionTime` first ([datex/content.md](../datex/content.md)).

**Vehicle positions** start with a full snapshot after every reconnect: dedup on
`(vehicleId, lastUpdated)`.

## 6. Pitfalls, each guarded by a check

| Pitfall | Check |
| ------- | ----- |
| Grain mismatch, so joins fan out | row count before = after each join |
| Operating day missing from the key | primary key uniqueness test |
| Duplicate updates counted | primary key uniqueness after dedup |
| Same quay twice in a journey | key on `Order`, not quay |
| Planned but no real-time | its own status: "no data" is neither "on time" nor "cancelled" |
| Real-time but no plan | count it: extra journey or broken id mapping |
| Actual time copied from aimed (some operators) | share of calls with actual = aimed, per operator and day |

## 7. Running it on the VM

8 GB RAM, one Spark job at a time, small Parquet reader batches for payload columns, and a
spill directory on `/data` rather than the small root disk. Details and evidence: [system/storage.md](../system/storage.md).

## Formats

| | Plan | Real-time |
| - | ---- | --------- |
| Entur / European | NeTEx (XML) | SIRI (XML): ET, SX, VM |
| Google | GTFS (CSV) | GTFS-RT (protobuf) |
| Roads (NPRA) | DATEX II location tables (XML) | DATEX II publications (XML) |

Entur's master data is NeTEx + SIRI; GTFS is derived and GTFS-RT partial, so we join NeTEx ↔ SIRI.
