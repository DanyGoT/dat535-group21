# Gold: what is built

One manual job, [`gold.py`](../../pipeline/gold.py), turns one operating day of published Entur
and DATEX Silver into four tables. A static [dashboard](../../dashboard/README.md) sums them over
several days into a stop map, a transfer explorer and a road-conditions comparison.

Up: [system overview](../report/overview.md#5-gold-answering-a-question) ·
Related: [Silver](silver.md#tables), [product plan](gold.md),
[evidence](../report/evidence/README.md#gold-with-road-data-2026-11-05-08-and-09)

## At a glance

| Table | One row is | Answers | Product in the [plan](gold.md#at-a-glance) |
| ----- | ---------- | ------- | ----------------------------- |
| `line_scorecard` | Line × operating day | Which lines run on time or cancel? | #2 punctuality scorecard |
| `stop_reliability` | Hub × local hour | Where and when do departures leave late? | Map for #3 delay hotspots |
| `transfer_reliability` | Hub × arriving line and origin × departing line and destination × local hour | Will my connection hold? | #8 transfer reliability |
| `departure_conditions` | Mode × local hour × four road-condition levels | Do late departures go with weather, roadworks or congestion? | First slice of #4 and #5 |

Every count is additive. Consumers sum days and compute rates from the sums. No table stores an
average or percentile that would have to be averaged later, except the scorecard's daily delay
percentiles, which stay per day.

## Shared rules

- **Inputs.** Entur Silver for operating day D (marker schema 2) and DATEX Silver for UTC
  observation day D (marker schema 1). Both markers must be `complete, ready=true`, or the run
  needs `--allow-partial` and is published `partial, ready=false`. Every table read must have
  exactly the marker's row count, and every row the marker's `_silver_run_id`.
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
  map and the conditions table split exactly the scorecard's timed and late departures.

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
(reported cancelled calls where boarding was allowed). Hub name and WGS84 position come from the
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
last calls, with names.

**Limits.** The curve pools connections with different planned gaps and assumes the gap change
does not depend on the gap. Walking time is the reader's input: NeTEx interchanges and NSR
walking times are not parsed. A connection that holds on paper may still be missed on foot.

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

## Dashboard

[`dashboard/build.py`](../../dashboard/build.py) reads only Gold Parquet and run markers, checks
each partition's row count and run ID and reconciles departures to the markers. It writes
`dashboard/dist/`, a static site that opens from disk or any static host:

- `data.js` holds hubs with at least 20 timed departures, the condition counts and line labels.
- `hubs/NN.js` are 32 shards of per-hub hours and line pairs with at least 20 observed
  connections, loaded when a hub is opened.

The road view compares each level with its reference within the same mode and hour, weighting
strata by the level's departures and skipping strata where the reference has fewer than 30.
Rush hour therefore cannot pass for congestion. Region and line are not controlled.

```sh
~/spark-env/bin/python dashboard/build.py --output-dir /data/dat535/prod \
  --date 2026-11-05 --date 2026-11-08 --date 2026-11-09
```

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

Both Silver markers for the date must exist: Entur under `silver/_runs/operating_date=D/` and
DATEX under `silver/_runs/datex/observation_date=D/`. Neither run contacts Entur or NPRA.

| Path under the output root | Contents |
| -------------------------- | -------- |
| `gold/<table>/operating_date=D/*.parquet` | The four tables |
| `gold/line_scorecard/operating_date=D/_preview/` | `report.md` (top 25 lines) and `scorecard.csv` |
| `gold/_runs/operating_date=D/<run_id>.json` | Lineage, totals, table sizes, condition coverage, runtime |
| `gold/_runs/operating_date=D/latest.json` | Publication and readiness marker |

The shared [publisher](../../pipeline/silver.py) replaces the selected day of all four tables and
rolls back ordinary failures. [Offline tests](../../pipeline/tests/test_gold.py) cover grains,
denominators, cancellations, quality exclusions, transfer pairing, condition levels including
NPRA's offset periods, partial input, reruns and mismatched lineage.

## Validated

Run on 2026-11-11 against prod Silver, writing to a separate output root. All three days had
both sources `complete, ready=true`. They are the only days where both exist: DATEX Silver is
missing for 11-06 and 11-07.

| Operating day | Gold run | Timed departures | Late | Observed connections | Elapsed | Peak RSS |
| ------------- | -------- | ---------------: | ---: | -------------------: | ------: | -------: |
| 2026-11-05 Thu | [`local-20261111T161642Z-fcdc0d31`](../report/evidence/gold-2026-11-05.json) | 1,624,438 | 37.5 % | 4,007,852 | 60 s | 2.84 GiB |
| 2026-11-08 Sun | [`local-20261111T161745Z-a11b99aa`](../report/evidence/gold-2026-11-08.json) | 600,491 | 22.7 % | 1,196,830 | 43 s | 2.26 GiB |
| 2026-11-09 Mon | [`local-20261111T161831Z-93e363a5`](../report/evidence/gold-2026-11-09.json) | 1,627,100 | 34.4 % | 4,004,644 | 64 s | 2.94 GiB |

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
