# Silver

Silver turns Bronze messages and archived references into reusable facts with explicit grains,
keys and quality flags. This page defines the contract, build rules and reruns.

Up: [overview](../report/overview.md#3-silver-for-entur-where-trust-is-built) ·
Related: [methodology](methodology.md#silver-where-trust-is-built),
[identifiers](../entur/identifiers.md), [evidence](../report/evidence/README.md)

## Rules in brief

- Define one grain per table and reduce versions before joining.
- Enrichment preserves fact counts. The plan/real-time merge preserves the union of keys.
- Use the newest reference archive on or before the requested day.
- Retain quality failures as flags or quarantine. Bronze keeps superseded versions.
- Parse narrow columns before shuffles. Reruns replace only the requested date.

## Tables

### Entur

Implemented in [`silver.py`](../../pipeline/silver.py). Keys below are unique within a partition.
The partition date completes each key. Quarantine has no asserted unique key.

| Table | One row represents | Key | Partition |
| ----- | ------------------ | --- | --------- |
| `stop_call` | Final reported state or unreported planned call | `journey_id, call_order` | `operating_date` |
| `journey` | Final metadata, including cancellation-only and plan-only journeys | `journey_id` | `operating_date` |
| `call_prediction` | Call-event state at a fixed cutoff | `journey_id, call_order, event, reference, lead_minutes` | `operating_date` |
| `vehicle_position` | Vehicle report after reconnect deduplication | `codespace, vehicle_id, last_updated_at` | `observation_date` |
| `sx_situation` | Native/content revision observed that day | `participant_id, situation_id, version_key` | `observation_date` |
| `quay`, `stop_place`, `line`, `operator` | Reference entity selected for the operating day | `id` | `operating_date` |
| `dated_journey` | Dated alias mapped to the normal journey key | `dated_journey_id` | `operating_date` |
| `quarantine` | Unreadable message or unusable identity | None | `operating_date` |

Observation dates are UTC receipt days. Vehicle IDs are scoped by codespace. Positions with
unknown operating dates remain valid. Reports can recur across observation days.
For SX, use the earliest `first_received_at` across partitions to measure first observation.

References are daily as-of snapshots, not valid-from/to history. `snapshot_date` identifies the
archive used. Highest numeric entity versions win, and conflicting ties fail uniqueness checks.
Facts keep IDs. Gold adds labels.

### Roads

Implemented in [`silver_datex.py`](../../pipeline/silver_datex.py). Every table uses UTC
`observation_date`, the poll day. Add it to each key below.

| Table | One row represents | Key within partition |
| ----- | ------------------ | -------------------- |
| `road_situation` | Record version with recurring windows and WGS84 locations | `record_id, version, version_at` |
| `road_situation_interval` | Applicable span clipped to that UTC day | `record_id, version, version_at, start_at` |
| `road_weather` | Station measurement with quantities as columns | `site_id, measurement_at` |
| `road_travel_time` | Section and five-minute period | `section_id, period_start_at` |
| `weather_site` | Latest observed station reference with daily sensor statistics | `id` |
| `road_section` | Latest observed section reference with WGS84 path and `measured_periods` | `id` |
| `quay_road_situation` | Quay within 500 m of sampled record geometry | `quay_id, record_id, version, version_at` |
| `quay_road_section` | Quay within 500 m of sampled section geometry | `quay_id, section_id` |
| `quay_weather_site` | Quay's nearest usable station, without a distance limit | `quay_id` |
| `datex_quarantine` | Unreadable poll, failed source delivery or unusable identity | No asserted unique key |

`has_measurement=false` means no travel-time measurement. `weather_site` includes `measurements`,
`stuck_quantities` and `usable`. Spatial links use the newest available Silver quay partition on
or before the day, recorded as `quay_snapshot_date`. Without quays, links are skipped.
They also require all three road fact/reference groups, so partial runs may omit links.

Gold decides relevance and acceptable distance. Nearby does not imply causation.
See [DATEX content](../datex/content.md#when-things-appear-what-the-gaps-mean) for source gaps
and [sensor caveats](../datex/content.md#sensor-values-to-distrust-2026-11-05-flagged-in-silver-never-changed).

## Reading safely

Outputs are zstd Parquet under `silver/<table>/<partition>=D/`. Consumers must hold
`flock /data/dat535/.spark.lock` while checking the marker and reading its partitions.

| Run | Readiness marker |
| --- | ---------------- |
| Entur | `silver/_runs/operating_date=D/latest.json` |
| DATEX | `silver/_runs/datex/observation_date=D/latest.json` |

Require `status=complete, ready=true`. This means source coverage and contract checks passed,
not that every journey reported or every row is clean. DATEX's three measurement feeds require
all 24 source hours, while its six-hourly reference feeds require a successful input.
Check the marker's table list and apply a stated `dq_issue` policy.
Individual `_SUCCESS` files are insufficient.

Publication stages and checks tables, sets `status=publishing, ready=false`, replaces partitions,
and writes the final marker last. Ordinary installation failures roll back. A killed process
can leave staging files and a `publishing` marker. Rerun the date under the same lock.

### Schema compatibility

- Entur writes schema **2**. `entity_index` changed from int to long. Rebuild schema-1 days
  before reading across versions.
- DATEX declares schema **1** in its marker, but shared `stage_table` stamps
  `_silver_schema_version=2` on its rows. These disagree in the current code.
- [`gold_scorecard.py`](../../pipeline/gold_scorecard.py) accepts only schema 1 in both marker
  and row checks. It cannot consume newly rebuilt Entur Silver. The tracked manual results cover
  2026-10-26's sample and 2026-10-29. See the [Gold example](gold-example.md#validated).

## Shared semantics

| Fields | Meaning |
| ------ | ------- |
| `planned_*_at` | Timetable clock plus day offset, converted from `Europe/Oslo` to UTC |
| `earliest_departure_at`, `latest_arrival_at`, `is_flexible` | Booking windows, never substituted for exact departures |
| `aimed_*_at`, `expected_*_at`, `actual_*_at` | Separate SIRI values, with no fabricated actuals |
| `arrival_delay_s`, `departure_delay_s` | Actual minus SIRI aimed in seconds, including negative delays |
| `quay_id`, `reported_quay_id`, `plan_quay_id` | Coalesced real-time/plan quay, source-reported quay and archived planned quay |
| `quay_changed` | Same journey position with different reported and planned quays |
| `actual_*_equals_aimed` | Source-quality evidence, not proof of punctuality |
| `mode`, `source_mode` | Lowercase vocabulary with `ferry` mapped to `water`, and original real-time mode |
| `direction_id`, `direction_type`, `direction_name` | SIRI code, NeTEx type and vehicle API name, without an assumed 1/2 mapping |

`has_plan`, `has_realtime` and `realtime_status` distinguish `matched`, `no_realtime` and
`realtime_only`. `journey_has_realtime` distinguishes cancellation-only messages from entirely
unseen journeys. A plan fallback never becomes a source-reported quay.

Missing real-time means neither on time nor cancelled. `service_alteration`, journey cancellation
and call cancellation remain separate. Silver computes no punctuality threshold, cancellation rate
or risk feature. Vehicle `speed` retains its published value. Its unit is
**UNVERIFIED** and it is not used in derived measures.

## How it is built

### Grain and plan join

[`merge_plan`](../../pipeline/silver.py) joins on operating day and journey ID, adding
`call_order` for calls. A quay can recur within a journey. Dated NeTEx aliases resolve to
`ServiceJourneyRef` and `OperatingDayRef`, preserving the source ID.

Vehicle reports retain journey/date and monitored-call references, but repeated reports must
be reduced before a call-level join. SX links through affected entities and validity,
not the stop-call key.

[`silver_reference.py`](../../pipeline/silver_reference.py) resolves calendars and stop mappings
before emitting plan facts. Flexible calendars use XML structure and shared-file scope.
Repeated assignment IDs retain each date rule. Ambiguous or nonexistent DST clocks become null
with an issue, while explicit offsets resolve them.

### Operating day and input window

ET reads completed UTC Bronze days `D-1, D, D+1`, then resolves and filters operating day `D`.
This covers pre-midnight reports, overnight services and following-day replay.
SX and vehicles read only UTC `D`. Updates received outside ET's window remain excluded on reruns.

[`normalize_journeys`](../../pipeline/silver.py) records date precedence in `date_source`:

1. `framed` uses the message's `DataFrameRef`.
2. `alias` uses the plan's dated journey mapping.
3. `id` uses a date embedded in the ID.
4. `first_call` uses the Oslo date of the first aimed call as a last resort.

Examples of embedded dates are `VYG:DatedServiceJourney:1614_DAL-DRM_26-11-05` and
Bane NOR's `660:2026-11-05`. The last two sources set `operating_date_derived`.
The first-call fallback can misdate overnight services. Other-day versions are filtered out.
Versions without a resolvable day are quarantined. Resolved extra or unplanned journeys remain
as `realtime_only` facts.

### Decoding and deduplication

[`decode_day`](../../pipeline/silver.py) caches typed UTC days at
`silver/_decoded/<feed>/source_date=D/`. The stamp includes Bronze run IDs, counts, bytes and a
hash of [`silver_parse.py`](../../pipeline/silver_parse.py). Changed inputs rebuild it.
Rows leave Python through `mapInArrow` and `arrow_decoder`, and call versions omit journey attributes.

[`parse_et`](../../pipeline/silver_parse.py) suppresses unchanged states within each task.
It remembers dropped copies and resets memory at input gaps. The residual is measured below.

For final calls, any actual arrival or departure wins over estimates. Remaining priority is
`recorded_at`, `received_at`, payload hash and `entity_index`, descending. Journey metadata is
selected independently. Vehicle reports deduplicate on their table key.

[`silver_datex_parse.py`](../../pipeline/silver_datex_parse.py) skips repeated snapshot hashes
within a task before parsing non-situation feeds. Situation snapshots change `publicationTime`,
so they emit sightings on every poll and full records once per version per task.
Sightings yield `first_seen_at`, `last_seen_at` and `snapshots_seen`.
`(record_id, version)` alone is insufficient.

### Prediction samples

[`build_predictions`](../../pipeline/silver.py) samples arrival and departure at 1/5/10/20/30
minutes before planned time, falling back to ET aimed, or before final actual time.
A version becomes eligible after both producer and receiver timestamps. Actuals terminate
estimate intervals, and late old replays cannot undo newer producer states already known.

Journey cancellation is sampled independently. It can yield null call-specific values with
journey provenance and flags, even without an earlier call update. No row means no eligible
estimate or cancellation state at that cutoff.

The `actual` reference and final `stop_call` states are retrospective outcomes, not prediction
features. Collector metadata supplies reference availability timestamps when present.
A same-day archive alone does not prove it was available at an earlier cutoff.

## Quality and quarantine

- Unparseable typed values become null with named `dq_issue` flags. Other suspect values
  remain flagged on facts or references. Delays below -30 min or above 6 h get
  `implausible_arrival_delay` or `implausible_departure_delay`.
- Unreadable messages and unusable keys enter quarantine with source locators and evidence.
  Quarantine deliberately retains duplicates.
- Contract errors block strict publication even with all source hours present. Examples include
  corrupt Bronze, unfamiliar shapes and GraphQL errors. Field flags alone do not block readiness.
- DATEX `source_delivery_failed` is a missed poll retained in quarantine, not a blocking contract
  error. Well-formed XML without expected feed content is blocking `unknown_message_shape`.
- Staging checks non-null unique keys, partition membership, counts and issues. Failed strict
  reruns preserve published tables.

Run records sit beside each marker as `<run_id>.json`. They include input coverage, Bronze run
IDs, decoded/table counts, issues, bytes and elapsed time. Entur also records reference
dates/SHA-256, version reduction, coverage and actual-equals-aimed counts per operator.
Contracts are exercised in [Entur tests](../../pipeline/tests/test_silver.py) and
[DATEX tests](../../pipeline/tests/test_silver_datex.py).

## Running it on the VM

Use the [production workflow](../../.github/workflows/dat535-prod.yml) and the existing
`~/spark-env` environment. Follow the [storage rules](../system/storage.md#memory) for memory,
spill on `/data` and the shared lock. Reruns use archived inputs.

| Run/input | Date used |
| --------- | --------- |
| Scheduled Bronze and DATEX Silver | Yesterday, UTC |
| Scheduled Entur Silver | Two days ago, operating date |
| Manual `bronze_date` | UTC input day for both Bronze jobs |
| Manual `silver_date` | Entur operating day and DATEX UTC observation day |

A `silver_date` dispatch runs both Bronze steps, but does not rebuild ET's entire three-day window.
Without `bronze_date`, those steps still ingest yesterday. Ensure the needed Bronze days exist.
A Bronze-only dispatch skips both Silver jobs. Repair missing Bronze days manually, then rerun Silver.

The [dev workflow](../../.github/workflows/dat535-dev.yml) uses one-hour Bronze and
`--allow-partial`, producing `status=partial, ready=false`. The flag permits incomplete or
contract-failing exploration output. It does not force partial status when all checks pass.

Local execution additionally needs Java 8/17, archived NeTEx/NSR, Bronze inputs and Arrow
dependencies. [`requirements-dev.txt`](../../requirements-dev.txt) omits PyArrow and pandas,
so installing that file alone is not a complete Silver environment.

## Evidence and measurements

The [evidence index](../report/evidence/README.md) links runs and experiments.
The complete-day 2026-10-29 record was produced on 2026-11-05 with schema 1, before the current
delay flags and decoder. It does not validate today's exact logic.

The [decoder comparison](../report/problems.md#2026-11-06--dropping-repeated-et-states-changed-650-predictions)
used 2026-10-29. No calls, journeys or predictions were missing. Vehicles and SX were identical.
Of 33.0 M predictions, 2 moved by 3 s and 100 had later `available_at` values.
The decoder's real-hour sample found 54 % repeated call versions, 96 % journey versions and
98 % `RecordedCall`s.

### Exploratory plan matching, 2026-10-22

One ET hour, `hour=06`, joined to national NeTEx on
`DataFrameRef, DatedVehicleJourneyRef, Order`. Plan order came from
`StopPointInJourneyPattern/@order`, with quay mapping through `ScheduledStopPoint` and
`PassengerStopAssignment`. This used an exploratory parser and a plan downloaded one day late.

| Outcome | Stop calls | Finding |
| ------- | ---------: | ------- |
| Key and quay match | 398,626 (97.9 %) | |
| Journey absent from plan | 2,654 | Mostly SKY, with the 22nd already gone from the export |
| No framed journey reference | 3,337 | VYG dated IDs and extra journeys |
| Same order, different quay | 1,728 | Platform changes or an unresolved mapping gap |
| Order absent from plan | 721 | Mostly FLT, affected by the single-pass parser |

FLT files list journeys before stop points, which the exploratory single-pass parser mishandled.
In this sample, 855 of 15,405 planned journeys revisited a quay. Also, 23.3 M call versions
collapsed to 407 k calls, about 57 versions each. ET aimed times support delays alone.
The archived plan adds unseen journeys and reference attributes.

### Planned journeys unseen in ET, 2026-10-23

Eight per-codespace NeTEx files, limited to journeys starting before 14:00 local.
These exploratory percentages measure reporting gaps, not the size of punctuality bias.

| | AKT | FLT | ATB | BRA | RUT | SKY | KOL | VYG |
| - | -: | -: | -: | -: | -: | -: | -: | -: |
| Planned | 1992 | 103 | 3520 | 1466 | 9097 | 6394 | 3072 | 752 |
| Never seen | 0 % | 0 % | 0.8 % | 0.8 % | 1.3 % | 3.0 % | 6.2 % | 8.0 % |

VYG had 36 of its 60 unseen trains marked `ServiceAlteration=cancellation/replaced` in NeTEx.
SKY also reported 234 ET journeys absent from that day's plan.

## Formats

| Source family | Plan/reference format | Real-time format |
| ------------- | --------------------- | ---------------- |
| Entur / European | NeTEx XML | SIRI XML, including ET, SX and VM |
| Google | GTFS CSV | GTFS-RT protobuf |
| Roads / NPRA | DATEX II location-table XML | DATEX II publication XML |

Entur's master data is NeTEx and SIRI. GTFS is derived and GTFS-RT has partial coverage,
so this pipeline joins NeTEx to SIRI. The vehicle collector uses GraphQL rather than SIRI VM.
See [Entur's catalog](../entur/sources.md#catalog).
