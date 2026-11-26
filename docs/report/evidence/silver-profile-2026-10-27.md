# Where a Silver day goes (2026-10-27)

One full prod Entur Silver run for operating day 2026-10-27, profiled on 2026-11-26 during the
Silver rebuild. [`silver_profile.py`](silver_profile.py) read the Spark status API every 30 s
(read-only, no Spark job of its own) and its `summary` mode gives the
[record](silver-profile-2026-10-27.json). Spark 3.5.0, `local[4]`, 4 GB driver, 32 shuffle
partitions, as every nightly run. The run took 68 min and decoded one new UTC day
(2026-10-28). The other two days of ET's window came from the decode cache.

Up: [evidence index](README.md) · Related: [Silver](../../pipeline/silver.md),
[`max_by` benchmark](latest-benchmark-2026-11-24.json)

## Time and disk by step

| Step (Spark SQL execution) | Time | Share | Shuffle written | Spilled to disk | Joins |
| -------------------------- | ---: | ----: | --------------: | --------------: | ----- |
| Decode UTC day 2026-10-28 (`mapInArrow`, 141 M rows) | 32 min | 47 % | 0 | 0 | none |
| `call_prediction` (33 M rows) | 18 min | 26 % | 68.6 GB | 70.4 GB | 4 sort-merge |
| Scope three decoded days to the operating day (141 M versions) | 7 min | 10 % | 0 | 0 | 1 broadcast |
| `vehicle_position`, its decode and scope | 3.6 min | 5 % | 2.0 GB | 1.2 GB | 3 broadcast |
| `stop_call` (1.87 M rows) | 2.9 min | 4 % | 1.8 GB | 8.9 GB | 4 broadcast, 1 sort-merge |
| NeTEx plan (RDD over zip members) | 2.5 min | 4 % | 0 | 0 | none |
| The other 71 executions | 2.2 min | 3 % | 0.9 GB | 0.8 GB | |

## Findings

- **Decoding is the bottleneck, and it is even.** Python XML decoding of one UTC day takes
  32 min on 4 cores. Its slowest task is 1.3 times its median, so more cores would scale it.
  The older estimate in the docs was 37 min per UTC day.
- **`call_prediction` is the second cost and almost all of the disk.** `asof_intervals` runs
  once per event (arrival, departure) over all 140 M call versions. Each run shuffles them
  twice: one window per call and moment, then one per call. That makes four shuffles of about
  57 GiB in memory each. With 32 partitions every reduce task sorts about 1.8 GiB, so the sorts
  spill 70 GB. This is the transient 57 GB seen in `spark-scratch` during the rebuild.
- **`stop_call` spills 8.9 GB**, as much as `latest` alone spilled over a full day in the
  [benchmark](latest-benchmark-2026-11-24.json).
- **References broadcast.** Every join to `quay`, `line`, `dated_journey` or the day's journeys
  (about 77,000) is a broadcast hash join. Sort-merge joins appear only where both sides are
  large: the full outer join of plan and real-time calls in `stop_call`, the journey join, and
  the four as-of joins in `call_prediction` (an equi-join on the call key with a time range).
- **No skew.** In every stage with at least 8 tasks, the slowest task is at most 2.7 times the
  median. That RUT and SKY dominate the calls does not show at call-key partitioning.
- **32 shuffle partitions fit every step but one.** `stop_call` and `vehicle_position` finish
  their shuffle stages in a few seconds per task. `call_prediction`'s shuffles are too large
  for 32.

## Not tuned now

Two changes would cut `call_prediction`'s shuffle and spill: one window per as-of step (order
each call's versions by moment and priority, so the two windows share one shuffle), and more
shuffle partitions for this step (`DAT535_SHUFFLE_PARTITIONS` already exists). Neither is made
before hand-in. The rebuild runs on frozen code until 27 Nov, the output would need a full-day
equality check, and the nightly run fits the VM as it is (68 min, 57 GB of transient scratch on
a volume with 96 GB free). It is the next tuning step, with this profile as its baseline.
