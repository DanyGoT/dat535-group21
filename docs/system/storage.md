# Storage and memory on the VM

How much we store, how long the disk lasts, what to delete by hand when it fills, and how to
run Spark without the VM running out of memory. Measured 2026-10-24/26 unless noted.

## The machine

| Resource | Size | Used by |
| -------- | ---- | ------- |
| CPU | 4 vCPU | Spark local mode uses all four |
| RAM | 7.9 GB + 4 GB swap | services ~1.5 GB; each Spark driver up to its heap (4 GB) + ~1 GB |
| `/` (root) | 38 GB, 40 % used | `/tmp` (Spark spill), Prometheus, Grafana, the runner, the venv |
| `/data` | 984 GB, 33 % used, network-mounted | landing, `prod/`, `dev/` outputs |

## What a day costs

| Dataset | Landing (gzip) | Bronze (Parquet) | Notes |
| ------- | -------------: | ---------------: | ----- |
| SIRI ET | 8.4–9.0 GB weekdays, 3.4–4.6 GB weekends | ~8.1 GB zstd (was ~22 GB snappy) | 85 % of everything |
| vehicle positions | 1.25–1.27 GB | not ingested yet | |
| DATEX | ~0.8 GB | ~0.53 GB | nearly all situations, see [datex/sources.md](../datex/sources.md) |
| NeTEx + NSR | 0.32 GB (280 + 41 MB zips) | not in Bronze yet | one download a day, see [entur/sources.md](../entur/sources.md) |
| SIRI SX | 1–2 MB | 2–4 MB | |
| **total** | **~10.9 GB** | **~8.7 GB** | ~19.5 GB/day, weekdays |

Bronze ET was 2.6× its own gzipped input: snappy barely compresses multi-MB XML strings. On one
real hour (2026-10-22 08 UTC, 589 MB gzipped), snappy wrote 1,621 MB and **zstd 558 MB**, in
the same time (126 s vs 129 s). Bronze now writes zstd.

## Runway

2026-10-26: 299 GB used of 984 GB. At ~19.5 GB/day the monitor's disk alert (90 %) fires around
**2026-11-25**. Grafana's **Disk full in** panel shows the live forecast. Assumes weekday
volumes every day (so it errs early) and small Silver and Gold.

## When it fills: delete by hand, cheapest loss first

Nothing deletes data automatically: we want to know what we give up. Delete whole day
directories, in this order, and note what you removed in [report/problems.md](../report/problems.md).

| Order | What | Frees | What is lost |
| ----- | ---- | ----: | ------------ |
| 1 | `dev/` outputs older than a week | 36 GB today (full-day dev runs of 2026-10-15/16) | nothing: test output |
| 2 | rerun old prod Bronze days with zstd (workflow_dispatch, one date each) | ~14 GB per pre-zstd day (2026-10-15…22: ~95 GB) | nothing |
| 3 | prod Bronze days older than Silver needs (keep the last week) | ~8 GB per day | nothing: rerun Bronze from landing |
| 4 | the oldest **ET** landing days | ~8.5 GB per day | raw messages; Entur's BigQuery still has the processed ET history ([entur/sources.md](../entur/sources.md)) |
| never | DATEX, SX, NeTEx and NSR landing | | small, and no archive exists anywhere |

```sh
du -sh /data/dat535/{dev,prod}/bronze/*/date=* | sort -h | tail     # what is big
rm -rf /data/dat535/dev/bronze/siri-et/date=2026-10-1[56]           # example: step 1
```

Keeping only a week of Bronze from late November leaves landing alone growing (10.9 GB/day),
which lasts until about the end of December.

## Memory

The kernel OOM killer stopped a Bronze JVM on **2026-10-22 18:13** (3.8 GB resident). Spark
jobs share 7.9 GB with the collectors, Grafana, Prometheus, the runner and any notebook.

| Rule | Why |
| ---- | --- |
| **One Spark job at a time.** Workflow steps run under `flock /data/dat535/.spark.lock`; dev and prod wait for each other instead of overlapping. | two 4 GB drivers do not fit |
| **Notebooks:** `spark.driver.memory` ≤ 2g, `spark.stop()` when done, and not around 03:00 UTC (prod Bronze). Wrap long jobs in the same `flock`. | a forgotten notebook driver holds its heap for days |
| **Read payloads in small batches:** `spark.sql.parquet.columnarReaderBatchSize=256` (set in `create_spark`). | reading one ET hour's payloads with the default 4096 killed a 3 GB driver; 256 read it in 23 s |
| **Lift Jackson's string cap:** `.option("maxStringLen", ...)` on every JSON read of landing. | ET pushes reach 22.8M characters, DATEX situations 28.6M; the default (20M) marked them corrupt |
| **No `partitionBy` on a constant column.** Write the day directory directly. | it sorts the whole day and spilled to `/tmp` until the root disk filled (2026-10-24) |

**Spill space.** `spark.local.dir` (Spark's temporary shuffle and sort files, not any table's
output) defaults to `/tmp` on the root volume. Bronze no longer spills. Silver will shuffle;
`/data` is network-mounted (problem log, 2026-10-24), so spilling there is slow but cannot fill
the root disk. Measure a Silver run's spill before choosing.

## The root disk

A process recording a terminal animation (`script … donut`) wrote 21.7 GB into a deleted file
from 2026-10-16 to 10-26 and filled `/` to 94 %, invisible to `du` (`lsof +L1` finds such
files). The monitor now checks `/` too. Keep personal tools and long recordings off the VM.

## Watching it

- Monitor: `disk` (data volume) and `root-disk` checks, both at `MONITOR_DISK_MAX_PCT` (90).
- Grafana: Disk free, Disk full in, Memory (available, swap).
