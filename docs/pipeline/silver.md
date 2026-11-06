# Silver

Silver turns Bronze (every message, as received) into trusted tables with a stated grain,
keys and quality flags, shared by every Gold product ([pipeline/gold.md](gold.md)). The Entur
tables are implemented in [`pipeline/silver.py`](../../pipeline/silver.py), the road tables in
[`pipeline/silver_datex.py`](../../pipeline/silver_datex.py); departure-risk Gold remains future
work. A manual [line-scorecard Gold example](gold-example.md)
has passed a complete-day VM run. A compact overview of the decisions, code and evidence is in the
[system overview](../report/overview.md). Keys and id quirks: [entur/identifiers.md](../entur/identifiers.md).

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

## 2. Tables

Implemented Entur contracts (schema version **1**):

| Table | One row = | Unique key within a partition | Partition |
| ----- | --------- | ----------------------------- | --------- |
| `stop_call` | final reported state, or an unreported planned call | `journey_id, call_order` | `operating_date` |
| `journey` | final journey metadata, including cancellation-only messages and plan-only journeys | `journey_id` | `operating_date` |
| `call_prediction` | state available at one fixed cutoff for one call event | `journey_id, call_order, event, reference, lead_minutes` | `operating_date` |
| `vehicle_position` | one vehicle report, reconnect duplicates removed | `codespace, vehicle_id, last_updated_at` | `observation_date` (UTC) |
| `sx_situation` | one native/content revision observed that UTC day | `participant_id, situation_id, version_key` | `observation_date` (UTC) |
| `quay`, `stop_place`, `line`, `operator` | an entity in the reference snapshot selected for that operating day | `id` | `operating_date` |
| `dated_journey` | a dated journey alias mapped to the normal journey key | `dated_journey_id` | `operating_date` |
| `quarantine` | an unreadable message or a decoded version with an unusable identity | source locator + `entity_json`; deliberately no dedup | run's `operating_date` |

The date is part of every table's full key. An SX revision or vehicle report can be observed
on more than one UTC day; take the earliest `first_received_at` across SX days to measure
first observation. Unknown vehicle operating dates stay null: valid positions still belong
to their **observation** day. Vehicle IDs are scoped by codespace, not assumed national.

Reference tables are **daily as-of snapshots**, not fabricated valid-from/to history. Their
`snapshot_date` identifies the actual archive used, which may precede the partition date.
Within an archive, the highest numeric entity version wins; conflicting values at that same
version fail the run. Dated aliases preserve both IDs for VYG and other dated references.

The road tables (schema version **1**), all partitioned by the UTC `observation_date` of the
polls, with their own marker `silver/_runs/datex/observation_date=D/latest.json`:

| Table | One row = | Unique key within a partition |
| ----- | --------- | ----------------------------- |
| `road_situation` | one version of one situation record seen that day, with `first_seen_at`, `last_seen_at`, its recurring windows and locations (WGS84) | `record_id, version, version_at` |
| `road_situation_interval` | one span of that UTC day in which the record applies | `record_id, version, version_at, start_at` |
| `road_weather` | one station at one measurement time, its quantities as columns | `site_id, measurement_at` |
| `road_travel_time` | one road section and 5-minute period; `has_measurement` is false when no vehicles were measured | `section_id, period_start_at` |
| `weather_site` | a station in the day's last snapshot, plus `measurements`, `stuck_quantities`, `usable` | `id` |
| `road_section` | a section in the day's last snapshot (path in WGS84), plus `measured_periods` | `id` |
| `quay_road_situation` | a record within 500 m of a quay | `quay_id, record_id, version, version_at` |
| `quay_road_section` | a section within 500 m of a quay | `quay_id, section_id` |
| `quay_weather_site` | a quay's nearest usable station, at any distance | `quay_id` |
| `datex_quarantine` | an unreadable poll | source locator; deliberately no dedup |

What the feeds' gaps mean, and which sensor values to distrust: [datex/content.md](../datex/content.md).
A nearby road event is not a cause of a delay, and a link is only "near": whether it matters is
Gold's question. The link tables use the newest quay snapshot on or before the day
(`quay_snapshot_date` in the run record) and are skipped before the first Entur Silver run.

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

**DATEX** polls return the full snapshot each time; repeated snapshots are byte-identical and
are skipped by `_bronze_payload_sha256` **before** parsing XML. A situation snapshot is never a
repeat (its `publicationTime` changes), so each poll yields a light sighting of every record and
the full record only once: `first_seen_at`, `last_seen_at` and `snapshots_seen` come from the
sightings. `(id, version)` alone is not unique ([datex/content.md](../datex/content.md)).

**Vehicle positions** start with a full snapshot after every reconnect: dedup on
`(codespace, vehicleId, lastUpdated)` within the UTC observation day.

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

The daily workflow runs Bronze and DATEX Silver for yesterday, then Entur Silver for **two days
ago**. A failed day is rerun by hand, with `bronze_date` or `silver_date` on the workflow. ET reads
completed Bronze partitions for `D-1, D, D+1`, then filters by the explicit operating day
(after dated-ID resolution). This conservative window covers pre-midnight reports, overnight
services and following-day replay. SX and positions read UTC `D` only; their grains are
observation-based. Updates received beyond the ET window are a remaining backfill limit.

**Decoding is the expensive part, so it happens once.** `silver/_decoded/<feed>/source_date=D/`
holds the typed versions of one UTC Bronze day, reused by all three operating days that read it
until Bronze or `DECODER_VERSION` changes. It is rebuildable at any time, so it is the first thing
to delete when disk runs short. Two more measured savings,
see `parse_et`: a state is emitted only when it differs from that entity's last one in the task
(54 % of call versions and 96 % of journey versions repeat, 98 % of `RecordedCall`s), and call rows
carry no journey attributes. Rows leave Python through Arrow (`mapInArrow`, `arrow_decoder`).

**The operating day of a version** is resolved in this order, `date_source` says which:
`framed` (the message's `DataFrameRef`), `alias` (the plan's dated journey), `id` (a date inside
the id, as in `VYG:DatedServiceJourney:1614_DAL-DRM_26-11-05` or Bane NOR's `660:2026-11-05`) and
`first_call` (the Oslo date of the first aimed time, for extra journeys that name no day). The last
two also set `operating_date_derived`. A version of another day is then filtered out as such; only
a version with no day at all goes to quarantine. Extra journeys and trains that are not in the
plan therefore appear as `realtime_only` facts instead of disappearing.

```sh
# Local example; Java 8/17 and the archived data must already be available.
DAT535_LANDING_DIR=/path/to/landing DAT535_OUTPUT_DIR=/path/to/dev DAT535_ENV=dev \
  uv run --python 3.11 --with-requirements requirements-dev.txt \
  python pipeline/silver.py --date 2026-10-26 --allow-partial
```

On the VM, use the workflow's **Run Entur Silver** step (same venv as Bronze, under
`flock /data/dat535/.spark.lock`). Manual production reruns take a separate `silver_date`
input: it is an operating date, while `bronze_date` is a UTC input date. A Bronze-only
backfill does not implicitly rerun an unrelated Silver day. Dev processes the one-hour
Bronze sample with `--allow-partial`; its marker always says `ready=false`.

### Quality, provenance and publication

- Typed UTC timestamps, dates, booleans and numeric fields have explicit schemas. An
  invalid value becomes null **with a named `dq_issue`**, never a silent correction.
- Nonempty `dq_issue` arrays remain on the fact/reference rows. Missing keys and unreadable
  messages live in `quarantine`, with their Bronze file/hash/run locator and decoded evidence.
  Superseded versions are the intentional reduction; Bronze retains all of them.
- Corrupt, incomplete or wrong-endpoint Bronze envelopes and invalid vehicle objects are
  contract failures even when every source hour exists. Strict runs fail with audit counts
  and leave the published day intact; `--allow-partial` retains the quarantine rows and
  publishes `ready=false`. Ordinary field-level flags remain countable without blocking
  readiness by themselves. GraphQL error arrays become `graphql_error` quarantine records.
- `has_plan`, `has_realtime` and `realtime_status` distinguish `matched`, `no_realtime` and
  `realtime_only`. `journey_has_realtime` also distinguishes a cancellation-only journey
  message from an entirely unseen journey. Missing real-time is an ordinary coverage state.
- `service_alteration`, journey cancellation and call cancellation stay separate. No
  punctuality threshold, cancellation rate, risk feature or Gold aggregate is computed.
- Every partition is staged, checked for unique/non-null keys, and counted before
  publication. Reference joins cannot multiply facts; the plan/RT full outer join explicitly
  preserves the union of their keys. Failed reruns preserve the published day.

Outputs are `silver/<table>/<partition>=D/*.parquet` (zstd). Run evidence is
`silver/_runs/operating_date=D/<run_id>.json`: source hours/status/counts and Bronze run IDs,
reference dates/SHA-256, version reduction, row counts, issue counts, bytes, elapsed time,
and actual-equals-aimed counts per operator. `latest.json` is the consumer's readiness gate.
Only `status=complete, ready=true` denotes complete input coverage and passed contracts;
individual flagged rows still require the consumer's stated treatment.

Consumers must hold the same VM lock while checking `latest.json` and reading the selected
partitions. Publication invalidates readiness, replaces the checked partitions, then publishes
the marker last; an ordinary install failure rolls back. A killed process can leave a
`publishing` marker and staging files: rerun that day. Do not use individual `_SUCCESS` files
as a multi-table readiness signal. Other operating dates are never replaced by that rerun.

### Shared semantics

| Field / condition | Meaning |
| ----------------- | ------- |
| `planned_*_at` | exact local timetable time + day offset, converted using `Europe/Oslo` |
| `earliest_departure_at`, `latest_arrival_at`, `is_flexible` | booking/flexible-service windows; never substituted for an exact departure |
| `aimed_*_at`, `expected_*_at`, `actual_*_at` | separate SIRI fields; missing actuals are never filled from estimates or the plan |
| `arrival_delay_s`, `departure_delay_s` | actual minus SIRI aimed, in seconds; negative delays remain valid. `implausible_*_delay` flags under -30 min or over 6 h (batch-stamped or wrong-day actuals) |
| `quay_id`, `reported_quay_id`, `plan_quay_id` | usable enriched reference, source-reported quay, archived planned quay; a missing report is never relabelled as source data |
| `quay_changed` | same journey position, different planned and reported quay; retain both |
| `actual_*_equals_aimed` | source-quality evidence, not proof of punctuality |
| `mode`, `source_mode` | lowercase NeTEx vocabulary (`ferry` → `water`), with the original RT mode retained |
| `direction_id`, `direction_type`, `direction_name` | SIRI code, NeTEx type and vehicle API name, respectively; no assumed 1/2 mapping |

Ambiguous/nonexistent local timetable clocks at DST transitions become null with an issue;
explicit offsets resolve them. Flexible calendar selection uses XML structure, with fallback
references resolved in their own shared-file scope. Repeated calendar-assignment IDs across
day types retain each date rule.

`call_prediction` samples arrival and departure separately at 1/5/10/20/30 minutes before
the archived planned time (ET aimed if absent) or the final actual time. A version is eligible
only after **both** producer and receiver timestamps; a later actual terminates its estimate
interval, and a late old replay cannot undo a newer producer state already known. Journey
cancellation at the cutoff is sampled independently, including messages
with no calls or with the first call update arriving after the cutoff. Such samples retain
null predicted/call-specific values and use journey provenance and quality flags. No row
means no eligible estimate/cancellation state at that cutoff.

The `actual` reference is retrospective and final `stop_call` states are outcomes. They must
not become pre-departure features. Facts also retain reference availability timestamps when
collector metadata supplies them: a same-day archive alone cannot prove that the schedule
was known at an earlier lookup time. Vehicle `speed` is kept as published; its unit remains
**UNVERIFIED** and is not used in a derived measure.

## Formats

| | Plan | Real-time |
| - | ---- | --------- |
| Entur / European | NeTEx (XML) | SIRI (XML): ET, SX, VM |
| Google | GTFS (CSV) | GTFS-RT (protobuf) |
| Roads (NPRA) | DATEX II location tables (XML) | DATEX II publications (XML) |

Entur's master data is NeTEx + SIRI; GTFS is derived and GTFS-RT partial, so we join NeTEx ↔ SIRI.
