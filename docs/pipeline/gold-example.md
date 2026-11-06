# Gold example: daily line reliability

Consumer: a transport authority or dashboard reader asking how each line performed on an
operating day. Implementation: [`gold_scorecard.py`](../../pipeline/gold_scorecard.py).
One row is **one line on one operating day**. Operator, public line code, name and mode
come from that day's reference snapshot. This is a manual prototype, not a scheduled product.

The simple browser viewer (branch `examples/gold-scorecard`, kept off `main`) presents these outputs
with day/line selectors, delay cards and a distribution bar. It uses the checked Gold
CSV/run record and displays the input's completeness status. The live example currently
shows the complete operating day 2026-10-29.

## How the datasets fit together

| Dataset | What Entur Silver already exposes | What another Gold product must do |
| ------- | --------------------------------- | --------------------------------- |
| SIRI ET + NeTEx | `stop_call` and `journey` retain both planned and reported services; shared operating date, journey ID and call order | Aggregate the right grain; distinguish missing reports from cancellations |
| NeTEx + NSR | Daily `line`, `operator`, `quay`, `stop_place` references; quay coordinates and parent stop place | Join labels on ID **and the snapshot's operating date**; group quays into a hub when needed |
| Vehicle positions | Normalised journey/date, line, vehicle and call references, coordinates and occupancy | Reduce repeated position reports before joining to calls; unknown journey/date stays unknown. UTC observation partitions are not operating-day partitions |
| SIRI SX alerts | Affected IDs, validity intervals, content revisions and first observation time | Match affected line/stop/journey and time; a broad line alert is not a one-to-one call record |
| DATEX weather | Still Bronze, not Entur Silver | Build `(site, measurement time, quantity)` Silver and station references, then a quay-to-site mapping with a distance limit and a time match |
| DATEX road situations / travel times | Still Bronze | Build record/section facts, normalise geometry projections, then map transit locations/segments to the road network and validity periods |
| MET/Frost | Not collected | A separate optional source, useful for coastal weather; not needed by this example |

The internal Entur joins are already available. DATEX can be added alongside these tables
without redesigning the transit facts, but it is not a direct ID join: location, time,
distance limits and road relevance need explicit rules. See [DATEX content](../datex/content.md)
and [external sources](../external/sources.md). A nearby road incident alone does not establish
the cause of a delay. Reduce multiple matches before joining them into a call-level dataset.

## What this example demonstrates

`journey` supplies service counts, `stop_call` supplies final timing observations, and
`line` / `operator` supply labels. Gold parses no XML and reads no landing or Bronze data.
Journey and call counts are aggregated separately before being combined: a journey with
30 calls still counts as one journey.

| Metric | Definition |
| ------ | ---------- |
| Planned journeys | `journey.has_plan` |
| Reported planned / unreported planned | Planned journeys with / without a real-time journey record |
| Real-time-only journeys | Reported journeys absent from the selected archived plan |
| Reported cancelled journeys | Real-time journey record explicitly marked cancelled |
| Timetable cancellations / replacements | Separate counts of the NeTEx `service_alteration`; do not add these to real-time cancellations |
| Scored departures | Reported calls with actual and aimed departures, usable delay, no call/journey cancellation, boarding not explicitly forbidden, and no departure-before-arrival or implausible-delay issue |
| Late share | Scored departures with `departure_delay_s >= 180`, divided by scored departures |
| Median / p90 delay | Approximate percentiles over those same scored departures, in seconds |
| Reporting coverage | Reported planned journeys / planned journeys; the analogous stop-call measure is also exported |

Zero measured departures produces null delay statistics and a null late share. Coordinates
or label issues do not discard otherwise valid timing evidence. Early departures and
actual-equals-aimed observations remain separate evidence columns. A missing real-time
record is unknown, never an inferred cancellation or punctual departure. The scorecard
does not estimate a future departure's risk.

## Run on the VM

Check `free -m` first. Use at least 4 GiB available RAM for the 2 GiB driver and overhead,
and stop any exploration Spark sessions. The native Spark runner uses the existing venv,
Spark 3.5.0 and Java setup; it installs nothing. Hold the same VM lock for the complete
Silver read and Gold write. The example's data and spill files live on `/data`.

```sh
cd /data/dat535/gold-scorecard-example/code
flock -n /data/dat535/.spark.lock \
  /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 2g \
  --conf spark.pyspark.python=/home/ubuntu/spark-env/bin/python \
  --conf spark.pyspark.driver.python=/home/ubuntu/spark-env/bin/python \
  --conf spark.local.dir=/data/dat535/gold-scorecard-example/spark-scratch \
  pipeline/gold_scorecard.py --date 2026-10-29 \
  --output-dir /data/dat535/prod
```

The original saved October 26 example uses a complete national plan plus bounded real-time
input at `/data/dat535/gold-scorecard-example/output`; rerunning it requires `--allow-partial`
and that date. Its missing reports describe the sample. The complete October 29 example uses
the ordinary `prod` Silver root above. Neither Gold run makes an Entur or NPRA request.

For complete Silver, use the ordinary environment output root (`DAT535_OUTPUT_DIR`) and
omit `--allow-partial`. The default refuses non-ready input. Exploration accepts a published
partial run but keeps Gold `ready=false`; a failed or publishing Silver marker is always
rejected. All four tables must agree with the marker's row counts, date, schema and run ID.
Reads declare the table root as Spark's `basePath` so partition dates are recovered even
when a reference snapshot does not repeat that column inside the Parquet files.

## Outputs and checks

Under the selected output root:

- `gold/line_scorecard/operating_date=D/*.parquet`: all line/day aggregates.
- `gold/line_scorecard/operating_date=D/_preview/report.md`: readable table of reported lines.
- `gold/line_scorecard/operating_date=D/_preview/scorecard.csv`: every line and metric.
- `gold/_runs/operating_date=D/<run_id>.json`: source Silver run, reconciled counts and runtime.
- `gold/_runs/operating_date=D/latest.json`: publication/readiness marker.

Preview files are in an underscore directory so Spark never attempts to read them as
Parquet. Only the small line aggregate is collected for export. Outputs are staged and
checked for unique line/day keys and totals equal to Silver before publication. The existing
Silver publisher provides partition replacement and rollback; other days are preserved.
Readers use the shared lock and readiness marker. A failure before publication preserves
the previous Gold day.

Offline tests in [`test_gold_scorecard.py`](../../pipeline/tests/test_gold_scorecard.py)
cover grains, denominators, cancellation categories, missing actuals, reference fan-out,
quality rules, partial inputs, reruns and mismatched source lineage.

## VM validation — 2026-11-04

The [generated report](../report/evidence/gold-line-scorecard-2026-10-26-sample.md) and
[run record](../report/evidence/gold-2026-10-26-sample.json) come from an actual VM execution
over the saved 2026-10-26 Silver sample, not fabricated output.

| Check | Result |
| ----- | ------ |
| Runtime | Spark 3.5.0, Python 3.11, Java 8; `local[2]`, 2 GiB driver, scratch on `/data` |
| Silver input reconciliation | 76,186 journeys and 1,855,992 stop calls, all accounted for |
| Gold reduction | 3,953 line/day rows, 115,281 Parquet bytes |
| Timing evidence in sample | 8 reported journeys, 107 scored departures, 31 at least 180 s late |
| Gold readiness | `partial`, `ready=false`; missing reports must not be interpreted as national service gaps |
| Duration | Gold recorded 12.959 s; complete process 15.69 s |
| Peak process RSS (`/usr/bin/time -v`) | 1,565,424 KiB, about 1.49 GiB; not a sum of all VM processes |
| RAM afterward | 6,494 MiB available; swap 990 MiB (991 MiB before), no additional swap pressure observed |
| Offline tests | All 5 passed after adding partition-only date coverage; final test process 25.37 s |

## Complete-day VM validation — 2026-11-05

The [complete Silver record](../report/evidence/silver-2026-10-29.json),
[Gold record](../report/evidence/gold-2026-10-29.json) and
[generated report](../report/evidence/gold-line-scorecard-2026-10-29.md) cover all required
closed input hours for operating day **2026-10-29**. Both layers are `complete`, `ready=true`.

| Check | Result |
| ----- | ------ |
| Silver grains | 76,631 journeys; 1,869,699 stop calls; 32,961,416 sampled prediction states |
| Other reusable Silver facts | 10,811,780 vehicle positions and 5,750 situation revisions |
| Gold | 3,992 line/day rows; 3,515 lines with reports; 72,786 reported journeys |
| Timing evidence | 1,618,184 scored departures; 573,915 at least three minutes late |
| Gold runtime | 17.286 s; complete process 21.78 s; peak process RSS 1,570,648 KiB |
| Full Silver runtime | 15,573.513 s, about 4 h 20 min, after the earlier day filter |
| Whole-service memory peak | 4,300,320,768 bytes, about 4.00 GiB, below the 5 GiB cap |
| Validation | All 29 Silver and 5 Gold tests passed; counts and source lineage reconcile |

The complete input still contains quality limitations: 52,423 planned calls had no real-time
report, and 4,441,088 ET update versions across the three-day source window could not resolve
an operating day and remain quarantined. These are not inferred cancellations. The typed
target-day call/journey version counts match the original unscoped baseline exactly.

The [week validation log](../report/evidence/silver-week-2026-10-26-to-2026-11-01.md) records
the timestamp bug found by the first full run, its fix and measured performance. The seven-day
backfill remains in progress; no weekly reliability result is claimed. The model and DATEX
enrichment remain separate future Gold work.
