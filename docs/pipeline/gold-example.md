# Gold example: daily line reliability

This manual line scorecard lets a transport authority or dashboard reader compare lines on one
operating day. [`gold_scorecard.py`](../../pipeline/gold_scorecard.py) implements it on `main`.

Up: [system overview](../report/overview.md#5-gold-answering-a-question) ·
Related: [Silver](silver.md#schema-compatibility), [product plan](gold.md),
[evidence](../report/evidence/README.md)

One row is **one line on one operating day**. The viewer is on branch
`examples/gold-scorecard`. It uses checked Gold CSV/run records, shows input completeness
and presents the complete 2026-10-29 example with day/line selectors and delay displays.

## What this example demonstrates

`journey` supplies service counts, `stop_call` supplies final timing, and `line`/`operator`
supply labels from the operating day's reference snapshots. Facts are aggregated separately
before joining, so a 30-call journey counts once. Gold reads only published Silver and parses no XML.

DATEX facts and spatial mappings also exist in [Silver](silver.md#roads), but this example does
not use them. Other products need explicit location/time rules and must reduce multiple
matches before joining calls. A nearby road incident alone does not establish causation.
MET/Frost remains an [optional source](../external/sources.md#overview).

## Metrics

| Metric | Definition |
| ------ | ---------- |
| Planned journeys | `journey.has_plan` |
| Reported / unreported planned | Planned journeys with / without a real-time record |
| Real-time-only journeys | Reports absent from the selected archived plan |
| Reported cancelled journeys | Real-time journeys explicitly marked cancelled |
| Timetable cancellations / replacements | Separate NeTEx `service_alteration` counts |
| Scored departures | Reported calls meeting the timing rules below |
| Late share | Departures with `departure_delay_s >= 180`, divided by scored departures |
| Median / p90 delay | Approximate percentiles over scored departures, in seconds |
| Reporting coverage | Reported planned / planned journeys, also exported for calls |

Timetable cancellations and replacements are not additive with reported cancellations.
Scoring requires actual and aimed departures and a non-null, non-NaN delay. It excludes
call/journey cancellations, boarding explicitly forbidden, `departure_before_arrival`
and `implausible_departure_delay`. Coordinate or label issues do not exclude timing.
Early departures and actual-equals-aimed observations have separate evidence columns.

Zero scored departures gives null delay statistics and late share. Missing reports are
unknown, never inferred cancellations or punctual departures. This scorecard does not
predict future risk.

## Run on the VM

**Schema limit.** [Gold](../../pipeline/gold_scorecard.py) accepts only Silver schema
version 1, including row metadata. [Current Silver](../../pipeline/silver.py) writes
version 2. The command below works with the saved schema-1 day, not newly rebuilt Silver.
`--allow-partial` does not bypass this limit.

Run from a repository checkout on the VM with its existing Spark 3.5.0, Java 8 and Python 3.11
venv. Check `free -m` for at least 4 GiB available RAM and stop exploration sessions.
Hold the shared lock through reading and publication. See
[storage](../system/storage.md#memory).

```sh
flock -n /data/dat535/.spark.lock \
  /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 2g \
  --conf spark.pyspark.python=/home/ubuntu/spark-env/bin/python \
  --conf spark.pyspark.driver.python=/home/ubuntu/spark-env/bin/python \
  --conf spark.local.dir=/data/dat535/spark-scratch \
  pipeline/gold_scorecard.py --date 2026-10-29 \
  --output-dir /data/dat535/prod
```

The saved October 26 sample root is `/data/dat535/gold-scorecard-example/output`.
Use that root, `--date 2026-10-26` and `--allow-partial` to rerun it. Its missing reports
describe bounded input, not national service gaps. Neither run contacts Entur or NPRA.

`--output-dir` defaults to `DAT535_OUTPUT_DIR`, then `output`. Gold requires complete,
ready Silver by default. `--allow-partial` accepts published partial input and keeps Gold
`ready=false`. Failed or publishing markers are always rejected.

These instructions use the prepared VM environment.
[`requirements-dev.txt`](../../requirements-dev.txt) alone is not a complete local setup
for current Silver, whose decoder imports PyArrow.

## Outputs and checks

Paths under the selected output root, where `D` is the operating date:

| Path | Contents |
| ---- | -------- |
| `gold/line_scorecard/operating_date=D/*.parquet` | All line/day aggregates |
| `gold/line_scorecard/operating_date=D/_preview/report.md` | Up to 25 reported lines |
| `gold/line_scorecard/operating_date=D/_preview/scorecard.csv` | Every line and metric |
| `gold/_runs/operating_date=D/<run_id>.json` | Source lineage, totals and runtime |
| `gold/_runs/operating_date=D/latest.json` | Publication/readiness marker |

All four input tables must match their Silver marker's counts, date, schema and run ID.
Reads set `basePath` to recover partition-only dates. Output keys must be unique and
journey/call totals must reconcile before publication. Only the small aggregate is collected
for export. Spark ignores the underscore preview directory.

The shared [publisher](../../pipeline/silver.py) replaces the selected day and rolls back
ordinary installation failures. Readers need the shared lock and readiness marker.
Other days remain intact. [Offline tests](../../pipeline/tests/test_gold_scorecard.py)
cover grains, denominators, cancellations, missing actuals, quality exclusions, reference fan-out,
partial input, reruns and mismatched lineage.

## Validated

These archived schema-1 results were committed in `5ff59c0`, before `abe085e` added the
`implausible_departure_delay` exclusion. The run records do not identify the exact source
revision used during execution. These are historical measurements, not a rerun of today's
logic. The saved reports' interpretation also omits that exclusion.

| Operating day | Executed | Silver input | Gold result | Gold runtime |
| ------------- | -------- | ------------ | ----------- | ------------ |
| 2026-10-26 sample | 2026-11-04 | 76,186 journeys, 1,855,992 calls | 3,953 rows, 115,281 Parquet bytes | 12.959 s |
| 2026-10-29 complete | 2026-11-05 | 76,631 journeys, 1,869,699 calls | 3,992 rows, 3,515 lines with reports | 17.286 s |

Sample Gold run `local-20261104T162507Z-4e6ebbc8` names Silver
`local-20261028T144237Z-1a77a24e`. It has 8 reported journeys, 107 scored departures
and 31 late departures, with `partial`, `ready=false`.
See the [Gold record](../report/evidence/gold-2026-10-26-sample.json),
[Silver record](../report/evidence/silver-2026-10-26-sample.json) and
[report](../report/evidence/gold-line-scorecard-2026-10-26-sample.md).

Complete Gold run `local-20261105T051100Z-0345df18` names Silver
`local-20261105T005115Z-2f3d11a3`. It has 72,786 reported journeys, 1,618,184 scored
departures and 573,915 late departures. Both layers are `complete`, `ready=true`.
See the [Gold record](../report/evidence/gold-2026-10-29.json),
[Silver record](../report/evidence/silver-2026-10-29.json) and
[report](../report/evidence/gold-line-scorecard-2026-10-29.md).

Complete input still includes 52,423 unreported planned calls and 4,441,088 quarantined ET
versions without a resolvable operating day across the three-day source window.

Process measurements retained from the original validation:

| Measurement | Sample, 2026-11-04 | Complete day, 2026-11-05 |
| ----------- | ----------------- | ---------------------- |
| Complete Gold process | 15.69 s | 21.78 s |
| Peak process RSS | 1,565,424 KiB, about 1.49 GiB | 1,570,648 KiB |
| RAM after sample | 6,494 MiB available | — |
| Swap after sample | 990 MiB, from 991 MiB | — |
| Sample offline tests | All 5 passed in 25.37 s | — |

RSS measures one process, not all VM services. No additional sample swap pressure was
observed. The [week log](../report/evidence/silver-week-2026-10-26-to-2026-11-01.md#successful-complete-day-retry)
retains full-day Silver timings, memory, tests and decoder-equivalence evidence.
The one-off week validation did not complete all seven days.
