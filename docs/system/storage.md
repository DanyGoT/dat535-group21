# Storage and memory on the VM

This guide covers capacity, cleanup and Spark settings. Disk readings are from 2026-11-11
09:22 UTC, and other sizes from 2026-10-24/26 unless dated.

Up: [architecture](architecture.md) · Related: [runbook](../../collector/README.md),
[evidence](../report/evidence/README.md)

## The machine

| Resource | Capacity and use |
| -------- | ---------------- |
| CPU | 4 vCPU |
| RAM | 7.9 GB + 4 GB swap. Services ~1.5 GB, Spark heap up to 4 GB plus ~1 GB overhead |
| `/` | 38 GB, 16 GB used, 41 %. Holds `/tmp`, monitoring tools, runner and venv |
| `/data` | 984 GB, 645 GB used, 290 GB free, 70 %. Network-mounted |

On 2026-11-11, landing occupied 231 GB, prod 308 GB and dev 48 GB.

## What a day costs

Weekday baseline before substantial Silver:

| Dataset | Landing | Bronze zstd |
| ------- | ------: | ----------: |
| SIRI ET | 8.4–9.0 GB weekdays, 3.4–4.6 GB weekends | ~8.1 GB |
| Vehicles | 1.25–1.34 GB weekdays, ~0.64 GB Sundays | ~1.0 GB, 0.38 GB Sunday |
| DATEX | ~0.8 GB | ~0.53 GB |
| NeTEx + NSR | 0.32 GB, ZIPs of 280 + 41 MB | Not loaded into Bronze |
| SIRI SX | 1–2 MB | 2–4 MB |
| Total | ~10.9 GB | ~9.7 GB |

Combined estimate ~20.5 GB/day. Displayed totals sum to ~20.6 GB/day.
ET accounted for 85 % of that baseline. Vehicle Bronze took ~1 minute/day.
See [DATEX costs](../datex/sources.md#cost-per-day) and [static sizes](../entur/sources.md#why-archive-timetables-and-stops).

ET Bronze used ~22 GB/day with snappy. On 2026-10-22 at 08 UTC, one hour took 589 MB
as gzipped landing input. Snappy Parquet used 1,621 MB in 126 s, while zstd used 558 MB in 129 s.
See the [compression experiment](../report/problems.md#2026-10-24--bronze-parquet-was-26-larger-than-the-gzipped-input).

## Runway

The 2026-10-26 forecast assumed weekday volumes and small Silver/Gold.
`(0.90 × 984 − 299) / 20.5 ≈ 28.6 days` predicted an alert around **2026-11-24**.
It used 299 GB occupied.

A second scenario kept only a week of Bronze from late November. With landing alone growing
at 10.9 GB/day, it predicted capacity until about the end of December.

These estimates exclude today's Silver footprint. No replacement calendar estimate is measured.
Grafana's **Disk full in** panel provides a live forecast.

## When it fills: delete by hand, cheapest loss first

Cleanup is manual. Delete whole days and record removals in the [problem log](../report/problems.md).
Paths are relative to `/data/dat535/`.

1. Remove `{dev,prod}/silver/_decoded/<feed>/source_date=YYYY-MM-DD/` folders older than three
   days. This frees ~1.9 GB/day, or 19 GB for ten days on 2026-11-06. Reruns decode again in
   ~37 min/UTC day.
2. Remove `dev/` outputs older than a week. October's 2026-10-15/16 tests occupied 36 GB.
3. Rebuild pre-zstd prod Bronze with `workflow_dispatch`, one date at a time. This saves
   ~14 GB/day without data loss. Days 2026-10-15…22 offered ~95 GB.
4. Remove prod Bronze older than Silver needs, keeping the last week. This frees ~9.7 GB/day
   and can be rebuilt from landing.
5. Remove the oldest ET landing days, freeing ~8.5 GB/day. Raw messages are lost, although
   [BigQuery](../entur/sources.md#bigquery-history-reference) retains processed ET.

Keep DATEX, SX, NeTEx and NSR landing. They are small and have no alternative archive.

```sh
du -sh /data/dat535/{dev,prod}/bronze/*/date=* | sort -h | tail
rm -rf /data/dat535/dev/bronze/siri-et/date=2026-10-1[56]  # step 2 example
```

## Memory

**One Spark job at a time.** Pipeline tests and Spark batch steps hold
`flock -w 10800 /data/dat535/.spark.lock`. Two 4 GB drivers do not fit with services.
The kernel killed a 3.8 GB Bronze JVM on 2026-10-22 at 18:13.

Notebooks use `spark.driver.memory` ≤ `2g` and finish with `spark.stop()`.
Long jobs take the lock. Avoid morning batch runs, which start later than 03:00.
See the [missed Bronze day](../report/problems.md#2026-11-05--a-long-silver-run-cost-us-a-day-of-bronze-and-nothing-said-so).

[create_spark](../../pipeline/bronze.py) supplies these settings:

| Setting | Evidence |
| ------- | -------- |
| `spark.driver.memory=4g`, overridable by `SPARK_DRIVER_MEMORY` | Shared RAM budget |
| `spark.sql.parquet.columnarReaderBatchSize=256` | Default 4096 killed a 3 GB driver. 256 read the hour in 23 s |
| `spark.sql.optimizer.canChangeCachedPlanOutputPartitioning=false` | SPARK-45282 lost join matches, 44 % versus 99.6 % on 2026-11-07 |

Landing reads need `.option("maxStringLen", 2**31 - 1)`. ET reached 22.8M characters
and DATEX 28.6M, above Jackson's 20M default. Avoid `partitionBy` on constants.
Write day directories directly to avoid sorting payloads onto `/tmp`.

[Entur Silver](../../pipeline/silver.py) defaults to `local[4]` and 32 shuffle partitions.
[DATEX Silver](../../pipeline/silver_datex.py) uses `local[3]` and four. Both spill under
`<output parent>/spark-scratch`, or `/data/dat535/spark-scratch` here. Overrides are
`SPARK_MASTER`, `DAT535_SHUFFLE_PARTITIONS` and `DAT535_SPARK_LOCAL_DIR`.
DATEX decoding temporarily sets both the Parquet reader batch and Arrow batch to one
snapshot to fit its large payloads in memory. Bronze retains Spark's `/tmp` default,
but direct writes no longer need the removed sort.

Silver stages narrow parsed data and checked tables under `silver/_staging/<run>/`.
Normal exits remove staging. Killed jobs can leave files and a non-ready
marker. Inspect leftovers and rerun manually. Consumers take the shared lock and check
`latest.json`, not `_SUCCESS`. See [Silver reading rules](../pipeline/silver.md#reading-safely).

## The root disk

During 2026-10-16…26 a long-running process held 21.7 GB in a deleted file, filling `/` to 94 %.
`du` missed it. `lsof +L1` finds open deleted files.

## Watching it

Monitor checks `disk` and `root-disk` against `MONITOR_DISK_MAX_PCT`, default 90 %.
Grafana shows Disk free, Disk full in, RAM and swap.
