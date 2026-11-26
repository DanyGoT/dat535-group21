# Gold: contract and tables

[`gold.py`](../../pipeline/gold.py) runs nightly in [prod CI](../../.github/workflows/dat535-prod.yml)
(and with `--allow-partial` in [dev CI](../../.github/workflows/dat535-dev.yml)), turning one
operating day of published Entur Silver and the same UTC day of DATEX Silver into seven additive
tables. The [dashboard](../../dashboard/README.md) sums all ready Gold days into a stop map, a
transfer explorer, a map of delay between stops and a road-conditions comparison.

Up: [system overview](../report/overview.md#5-gold-answering-a-question) ·
Related: [Silver](silver.md#tables), [Gold scope & future work](gold.md),
[evidence](../report/evidence/README.md#gold-with-road-data-2026-11-05-08-and-09)

## At a glance

| Table | One row is | Answers | Product in the [plan](gold.md#at-a-glance) |
| ----- | ---------- | ------- | ----------------------------- |
| `line_scorecard` | Line × operating day | Which lines run on time or cancel? | #2 punctuality scorecard |
| `stop_reliability` | Hub × local hour | Where and when do departures leave late? | Map for #3 delay hotspots |
| `transfer_reliability` | Hub × arriving line and origin × departing line and destination × local hour | Will my connection hold? | #8 transfer reliability |
| `line_stop_delays` | Hub × line × arrival or departure × trip end × local hour | How late does this line arrive or leave here? | Input to #8 across nearby stops |
| `segment_delay` | Line × hub × next hub × local hour | Where is delay gained, and where recovered? | #3 delay hotspots |
| `departure_conditions` | Mode × local hour × four road-condition levels | Do late departures go with weather, roadworks or congestion? | First slice of #4 and #5 |
| `countdown_accuracy` | Line × reference × lead time | How far was the departure time shown from the actual one? | #6 countdown accuracy |

Every count is additive. Consumers sum days and compute rates from the sums. No table stores an
average or percentile that would have to be averaged later, except the scorecard's daily delay
percentiles, which stay per day.

## Shared rules

- **Inputs & missing-DATEX policy.** Entur Silver for operating day D (marker schema 2) is
  required; DATEX Silver for UTC observation day D (marker schema 1) is used when published.
  Present markers must be `complete, ready=true`, or the run needs `--allow-partial` and is
  published `partial, ready=false`. If DATEX Silver does not exist for day D, Gold still publishes
  the six Entur tables `ready=true`, skips `departure_conditions`, and records
  `datex_status: "missing"` and `skipped_tables: ["departure_conditions"]` in `latest.json`.
  Every table read must have exactly its marker's row count and `_silver_run_id`.
- **Timed departure.** One definition for every table: reported, not cancelled, boarding not
  forbidden, actual and aimed departure present, delay not NaN, and no `departure_before_arrival`
  or `implausible_departure_delay` flag. Late means `departure_delay_s >= 180`.
- **Hub.** A quay's NSR stop place, or that stop place's multimodal parent when it has one. A bus
  terminal and the railway station under the same parent are one hub, so bus-to-train transfers
  are found. Only 2,432 of 58,058 stop places had a parent on 2026-11-09.
- **Hours** are Europe/Oslo local hours of the aimed time.
- **Unknowns.** Missing real-time is unknown, never on time and never cancelled. Calls without a
  quay go to the `__unknown_hub__` row, lines without an ID to `__unknown_line__`.
- **Reconciliation before publication.** Scorecard journeys and calls equal Silver's rows. The
  map and the conditions table split exactly the scorecard's timed and late departures. The
  departure rows of `line_stop_delays` hold exactly the scorecard's timed departures and the
  map's cancellations. No reference and lead time of `countdown_accuracy` holds more predictions
  than the scorecard has timed departures, and every prediction is in its histogram.

## `line_scorecard`

Unchanged from the first example, apart from the shared definitions above. Journey and call facts
are aggregated separately before joining, so a 30-call journey counts once.

| Metric | Definition |
| ------ | ---------- |
| Planned / reported / unreported planned journeys | `journey.has_plan`, `has_realtime` and both |
| Real-time-only journeys | Reports absent from the selected archived plan |
| Reported cancelled journeys | Real-time journeys explicitly marked cancelled |
| Timetable cancellations / replacements | NeTEx `service_alteration`, not additive with reported cancellations |
| Late share, median and p90 delay | Over timed departures; approximate percentiles in seconds |
| Reporting coverage | Reported planned / planned, for journeys and calls |

Zero timed departures gives null rates. A preview `_preview/report.md` and `scorecard.csv` sit
beside the Parquet.

## `stop_reliability`

Timed departures per hub and hour, with `late_departures`, six delay bins (more than a minute
early, within a minute, 1–3, 3–5, 5–10 and 10+ minutes late) and `cancelled_departures`
(reported cancelled calls where boarding was allowed, including planned calls of a journey
cancelled as a whole). Hub name and WGS84 position come from the
operating day's `stop_place` snapshot.

## `transfer_reliability`

A transfer is observed, not modelled from two separate delay distributions:

1. Arrivals are calls after a journey's first call, alighting allowed, with an aimed (or planned)
   arrival. Departures are calls before its last call, boarding allowed.
2. At each hub, each arrival is paired with the **first** departure of every other line and
   destination aimed 0–15 minutes later. The join is an equi-join on 15-minute buckets, the
   arrival exploded into its own and the next bucket, then a window keeps the first departure.
3. When both calls have actual times, the **gap change** is the departure's delay minus the
   arrival's delay, floored to whole minutes and clipped to −11..+10. `gap_change_counts[i]`
   counts change `i − 11`.
4. A connection with *b* spare minutes (planned gap minus walking time) holds when the change is
   at least −b. The share that held is `sum(counts[11 − b:]) / sum(counts)`, exact for b = 0..10.
   The dashboard draws it as a curve over b and by hour.

`cancelled_connections` has either call cancelled. `unknown_connections` lacks an actual time or
carries an implausible-delay flag. Origin and destination are the hubs of the journeys' first and
last calls, with names, or `__unknown_hub__` when that quay is not in NSR. Delays keep their
fractional seconds; only the gap change is floored.

**Limits.** The curve pools connections with different planned gaps and assumes the gap change
does not depend on the gap. Walking time is the reader's input: NeTEx interchanges and NSR
walking times are not parsed. A connection that holds on paper may still be missed on foot.

## `line_stop_delays`

Delays per line at a hub, for transfers between two hubs that Gold does not pair: a bus stop and a
railway station a short walk apart without a shared NSR parent.

- **Arrivals** are calls with an aimed (or planned) arrival, alighting allowed. They are timed
  when reported, not cancelled, with an `arrival_delay_s` and no `implausible_arrival_delay` flag.
  `end_id` is the journey's origin hub.
- **Departures** are calls with an aimed (or planned) departure, boarding allowed. They are timed
  by the shared rule above. `end_id` is the journey's destination hub.
- `calls` counts both kinds, `cancelled_calls` the reported cancellations among them.
  `delay_counts[i]` counts timed calls whose delay floored to whole minutes is `i − 10`, clipped
  to −10..+30. Calls neither timed nor cancelled are unknown.

## `segment_delay`

A segment is a run between two consecutive calls of a journey, mapped to their hubs. Its
**delay change** is the arrival delay at the next call minus the departure delay at this one, so
time spent at stops is left out. Positive is delay gained, negative is delay recovered.

| Column | Definition |
| ------ | ---------- |
| `segments` | Runs between the two hubs, with or without real-time |
| `timed_segments` | Runs with a usable departure delay here and arrival delay at the next call: reported, neither call cancelled, no `implausible_*_delay` or `departure_before_arrival` flag |
| `delay_change_s` | Sum of the timed runs' changes in seconds, each clipped to ±15 min |
| `gained_1_min_plus`, `recovered_1_min_plus` | Timed runs that gained, or recovered, a minute or more |

Mean change per run is `delay_change_s / timed_segments`, summed over days first. Hour is the
local hour of the aimed departure. Runs within one hub, such as a bus terminal to its railway
station, and runs to or from a quay not in NSR are left out: the map cannot draw them. Hub names
and positions for both ends come from the day's `stop_place` snapshot.

**Limits.** Running time includes waiting at signals and traffic lights between stops; it does
not separate road from vehicle. Where the timetable adds slack before a timing point, recovery is
planned. Operators that copy aimed into actual times show no change. Not yet run on real Silver.

## `departure_conditions`

Each timed departure gets four levels at its quay and aimed time, then rows are counted per mode,
hour and level combination.

| Dimension | Levels | Source rule |
| --------- | ------ | ----------- |
| `precipitation` | dry < 0.1 mm/h, light < 1, heavy ≥ 1 | Hourly max at the quay's nearest usable station within 10 km |
| `road_surface` | freezing ≤ 0 °C, above_zero | Hourly min surface temperature, same station |
| `road_situation` | incident, roadworks, none | Worst situation interval active at that time, linked to the quay within 500 m |
| `traffic` | free_flow < 1.2, slow < 1.5, congested ≥ 1.5, unmeasured, no_section | Max `congestion_ratio` on sections within 500 m in that 5-minute period |

`unknown` means the departure lies outside DATEX's UTC day, or no usable value exists. Weather
excludes `stale_measurement`, the matching `suspect_*` flag and stuck precipitation sensors.
`incident` covers accidents, obstructions, roadside assistance, equipment faults and road
condition records. Every other record type counts as roadworks or traffic management.

**Limits.** Night departures before 01:00 local fall outside the DATEX day. Road stations sit
inland, so most ferry quays get `unknown` weather. Only 202 travel-time sections exist, so most
departures are `no_section`. Proximity is not causation.

## `countdown_accuracy`

How far the departure time a display or journey planner showed was from the actual departure.
It reads Silver's `call_prediction`: the state known 1, 5, 10, 20 and 30 minutes before each
call's planned and actual time, with no later knowledge
([`asof_intervals`](../../pipeline/silver.py)).

| Column | Definition |
| ------ | ---------- |
| `reference` | `actual`: N minutes before the vehicle left, the countdown. `planned`: N minutes before the timetable time, what a traveller looking it up then saw |
| `lead_minutes` | 1, 5, 10, 20 or 30 |
| `predictions` | Timed departures (shared rule) with a departure time known at that cutoff |
| `error_counts[i]` | Predictions whose error, time shown minus actual departure floored to whole minutes, is `i − 10`, clipped to −10..+10 |

A positive error means the vehicle left before the time shown, so a traveller trusting it could
miss it. Within a minute either way is `error_counts[9] + error_counts[10]`. A cancellation state
shows no time and is left out; a departure with nothing known at a cutoff has no row there. Labels
come from `line_scorecard` on the same line and day.

**Limits.** Where an operator copies aimed into actual times, the error is measured against the
timetable. Only the five sampled lead times exist. The `actual` reference is retrospective: nobody
can look up "10 minutes before it leaves" in advance.

## Dashboard

[`dashboard/build.py`](../../dashboard/build.py) reads only Gold Parquet and run markers, checks
each partition's row count and run ID and reconciles departures and timed runs to the markers. It
accepts only Gold marker schema 4: days published before `countdown_accuracy` existed must be
rerun. It does not read `countdown_accuracy`. It writes
`dashboard/dist/`, a static site that opens from disk or any static host:

- `data.js` holds hubs with at least 20 timed departures, the condition counts and line labels.
- `segments.js` holds hub pairs with at least 30 timed runs over all lines and days, loaded when
  the *Where delay grows* view opens. Each direction is drawn on its right-hand side of travel,
  coloured by mean change per run; the panel ranks pairs by change per run or in total. A filter
  picks the local time of day of the aimed departure (00–06, 06–09, 09–15, 15–18, 18–24), so the
  file keeps the four sums per band rather than one daily total, 5.4 MB for 7 days. The map
  draws only pairs in view, at most the 4,000 with the largest total change, and those within
  15 s per run only from city zoom: all 45 k pairs of 7 days at once made it lag.
- `hubs/NN.js` are 32 shards of per-hub hours, line pairs with at least 20 observed
  connections, and line delays with at least 20 timed calls, loaded when a hub is opened. Line
  delays are summed into five local time-of-day bands (00–06, 06–09, 09–15, 15–18, 18–24),
  because one line at one stop runs too rarely for single hours. Each hub also gets an "any
  line" entry per direction.

The road view compares each level with its reference within the same mode and hour, weighting
strata by the level's departures and skipping strata where the reference has fewer than 30.
Rush hour therefore cannot pass for congestion. Region and line are not controlled.

Shift-click (or "Compare with a nearby stop") puts two hubs side by side. The reader picks the
arriving line at one, the leaving line at the other, and a time-of-day band. Gold pairs calls
only within one hub, so the page *estimates* that transfer from `line_stop_delays`: the arriving
line's arrival delays against the leaving line's departure delays in the same band, taken as
independent, each delay spread evenly over its floored minute. With *k* whole minutes left
after spare minutes and both delays, the connection holds for k ≥ 1 and half the time for k = 0.
Cancellations are reported beside the estimate, not inside it, as in the transfer explorer.

```sh
flock -n /data/dat535/.spark.lock ~/spark-env/bin/python dashboard/build.py --output-dir /data/dat535/prod \
  --all-ready
```

Pass `--all-ready` to include every complete, ready schema-3 Gold day, or repeat `--date YYYY-MM-DD`
for specific days. Days where `departure_conditions` was skipped are included in the stop,
transfer and segment views and omitted from the road-conditions cube.

It holds the Spark lock for the whole build: a Gold rerun replaces its tables one by one, so a
build running beside it could mix two runs that the run-ID check before reading cannot see.

The builder reads the transfer table shard by shard with a filtered Arrow scan. Reading all
three days at once peaked at 5–7 GB. See the [problem log](../report/problems.md#2026-11-11--the-dashboard-builder-needed-more-memory-than-the-spark-job).

## Run on the VM

Run from a repository checkout with the existing Spark 3.5.0, Java 8 and Python 3.11 venv. Check
`free -m` for at least 4 GiB available RAM and hold the shared lock. See
[storage](../system/storage.md#memory).

```sh
flock -n /data/dat535/.spark.lock \
  /opt/spark/bin/spark-submit --master 'local[3]' --driver-memory 3g \
  --conf spark.pyspark.python=/home/ubuntu/spark-env/bin/python \
  --conf spark.pyspark.driver.python=/home/ubuntu/spark-env/bin/python \
  --conf spark.local.dir=/data/dat535/spark-scratch \
  pipeline/gold.py --date 2026-11-09 --output-dir /data/dat535/prod
```

Reruns are manual, one `--date` at a time, like every layer. A `silver_gold_date` dispatch of the prod
workflow also reruns Gold for that day, after its Silver.

The Entur Silver marker for the date must exist under `silver/_runs/operating_date=D/`. The DATEX
marker under `silver/_runs/datex/observation_date=D/` is optional: without it Gold skips
`departure_conditions`. Gold never contacts Entur or NPRA.

| Path under the output root | Contents |
| -------------------------- | -------- |
| `gold/<table>/operating_date=D/*.parquet` | The seven tables |
| `gold/line_scorecard/operating_date=D/_preview/` | `report.md` (top 25 lines) and `scorecard.csv` |
| `gold/_runs/operating_date=D/<run_id>.json` | Lineage, totals, table sizes, condition coverage, runtime |
| `gold/_runs/operating_date=D/latest.json` | Publication and readiness marker |

The shared [publisher](../../pipeline/silver.py) replaces the selected day of all seven tables and
rolls back ordinary failures. [Offline tests](../../pipeline/tests/test_gold.py) cover grains,
denominators, cancellations, quality exclusions, transfer pairing, line delays, segment delay
change, condition levels including NPRA's offset periods, countdown error, partial input, reruns
and mismatched lineage.

## Validated

These are schema-2 runs from before `line_stop_delays`, `segment_delay` and `countdown_accuracy`;
the dashboard builder needs schema-4 runs, so the three days must be rerun.

Run on 2026-11-11 against prod Silver, writing to a separate output root. All three days had
both sources `complete, ready=true`. They are the only days where both exist: DATEX Silver is
missing for 11-06 and 11-07.

| Operating day | Gold run | Timed departures | Late | Observed connections | Elapsed | Peak RSS |
| ------------- | -------- | ---------------: | ---: | -------------------: | ------: | -------: |
| 2026-11-05 Thu | [`local-20261111T192137Z-e426fdeb`](../report/evidence/gold-2026-11-05.json) | 1,624,438 | 37.5 % | 4,008,473 | 69 s | 2.94 GiB |
| 2026-11-08 Sun | [`local-20261111T192251Z-e4cf5972`](../report/evidence/gold-2026-11-08.json) | 600,491 | 22.7 % | 1,197,122 | 46 s | 2.48 GiB |
| 2026-11-09 Mon | [`local-20261111T192339Z-428817a6`](../report/evidence/gold-2026-11-09.json) | 1,627,100 | 34.4 % | 4,005,340 | 69 s | 2.88 GiB |

A weekday writes about 1.78 M transfer rows (21 MB), 406 k stop rows (7 MB), 4 k scorecard rows
and 2.5 k condition rows.

Over the three days, nationally, a connection held 52 % of the time with no spare minute, 77 %
with 2, 84 % with 3 and 92 % with 5. Matched on mode and hour, departures near congested
sections were late 10.0 percentage points more often than near free-flowing ones. Heavy
precipitation added 1.1 points, roadworks 2.0 and incidents 2.1. Freezing surfaces went with
1.7 points *fewer* late departures, which more likely reflects where and when it freezes than an
effect of frost. The [dashboard screenshots](../report/evidence/README.md#gold-with-road-data-2026-11-05-08-and-09)
show these views.

Earlier schema-1 runs of the scorecard alone, 2026-10-26 (sample) and 2026-10-29, remain in the
[evidence](../report/evidence/README.md#complete-day-2026-10-29) as history. They predate the
implausible-delay exclusion and the current job.
