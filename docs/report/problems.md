# Problem log

This log keeps one entry per problem, newest first, with run IDs, log lines, measurements, fixes and
lessons for the report's reflection ([PROJECT.md](../../PROJECT.md), section 7).
Entries record what happened on their dates, and [README.md](../../README.md) owns the current state.

Up: [overview](overview.md) · Related: [evidence](evidence/README.md)

Entries:

| Date | Incident |
| ---- | -------- |
| 2026-11-11 | [Dashboard builder memory](#2026-11-11--the-dashboard-builder-needed-more-memory-than-the-spark-job) |
| 2026-11-11 | [Travel-time periods off by 10 ms](#2026-11-11--npra-travel-time-periods-start-10-ms-after-the-boundary) |
| 2026-11-09 | [Empty DATEX delivery](#2026-11-09--failing-loudly-on-an-empty-snapshot-would-have-blocked-a-day-for-good) |
| 2026-11-07 | [Cached-data join lost matches](#2026-11-07--a-spark-bug-lost-join-matches-against-cached-data) |
| 2026-11-07 | [DATEX decode heap exhaustion](#2026-11-07--datex-silver-hung-after-running-out-of-heap) |
| 2026-11-06 | [ET deduplication changed predictions](#2026-11-06--dropping-repeated-et-states-changed-650-predictions) |
| 2026-11-05 | [Shared Spark lock blocked Bronze](#2026-11-05--a-long-silver-run-cost-us-a-day-of-bronze-and-nothing-said-so) |
| 2026-11-05 | [SX timestamp overflow](#2026-11-05--a-minimum-date-sx-timestamp-overflowed-during-utc-conversion) |
| 2026-11-04 | [Gold partition discovery](#2026-11-04--gold-must-discover-dates-from-silver-partition-paths) |
| 2026-10-29 | [DATEX writer heap exhaustion](#2026-10-29--datex-bronze-ran-out-of-heap-on-its-first-full-day) |
| 2026-10-28 | [Silver contract gaps](#2026-10-28--missing-and-malformed-real-time-data-crossed-silver-contract-boundaries) |
| 2026-10-28 | [NeTEx scope and versions](#2026-10-28--national-netex-references-need-scope-and-native-versions) |
| 2026-10-24 | [Bronze compression](#2026-10-24--bronze-parquet-was-26-larger-than-the-gzipped-input) |
| 2026-10-24 | [Bronze reader heap exhaustion](#2026-10-24--reading-bronze-payloads-killed-the-spark-driver) |
| 2026-10-24 | [Largest ET pushes marked corrupt](#2026-10-24--the-largest-et-pushes-were-marked-corrupt) |
| 2026-10-24 | [Bronze filled root disk](#2026-10-24--bronze-ran-out-of-disk-on-the-root-volume) |
| 2026-10-23 | [DATEX response assumptions](#2026-10-23--datex-did-not-behave-as-its-specification-suggested) |

Entry template:

```markdown
## YYYY-MM-DD — Short title

- **Area:** receiver / monitor / Bronze / Silver / Gold / CI/CD / VM
- **Symptom:** what we saw.
- **Evidence:** run ID, log excerpt, measurements.
- **Cause:** why it happened.
- **Fix:** what we changed (commit/PR).
- **Trade-off:** what the fix costs us, if anything.
- **Lesson:** what we take away for the design.
```

---

## 2026-11-11 — The dashboard builder needed more memory than the Spark job

- **Area:** Gold
- **Symptom:** The first dashboard build over three Gold days peaked at 5.2 GB RSS in pandas.
  Moving the aggregation to Arrow made it worse: 7.1 GB after 35 s, then a join type error.
  The Gold Spark job that wrote the same data peaked at 2.9 GB.
- **Evidence:** `/usr/bin/time -f "%e s, %M KB"` on the VM: 114 s and 5,192,164 KB in pandas,
  35 s and 7,056,816 KB in Arrow. Input: 4.17 M `transfer_reliability` rows, each with a
  22-element histogram, about 49 MB of Parquet.
- **Cause:** Decoding all rows at once materialises every string key and every histogram. Pandas
  holds the histograms as Python objects, and a hash group-by over five string keys copies them again.
- **Fix:** [dashboard/build.py](../../dashboard/build.py) builds each of its 32 output shards from
  its own filtered scan (`pyarrow.dataset` with `hub_id in shard`). The check of run IDs and row
  counts reads only `_gold_run_id`, once per table. Peak 2.1 GB, 166 s, identical output.
- **Trade-off:** 32 scans of the same files, because hubs are spread over shards by size and
  Parquet statistics cannot skip row groups for them.
- **Lesson:** A small driver-side step is not small when it reads a fact-sized table. Bound it
  by the size of what it writes.

## 2026-11-11 — NPRA travel-time periods start 10 ms after the boundary

- **Area:** Gold
- **Symptom:** The first Gold run classed every departure near a road section as `unmeasured`.
  Not one of 316,642 found a travel-time period, although 58 % of periods had a measurement.
- **Evidence:** Gold run on 2026-11-09, `condition_levels.traffic`: `no_section` 1,305,872,
  `unmeasured` 316,642, nothing else. Silver `road_travel_time` for that day: all 58,176
  `period_start_at` values end in `:00.010` or `:05.010`.
- **Cause:** The join matched a departure's 5-minute floor to `period_start_at` exactly. NPRA's
  periods start 10 ms after the boundary.
- **Fix:** Both sides are joined on `floor(epoch / 300)` in
  [pipeline/gold.py](../../pipeline/gold.py), and the test fixture now carries the 10 ms offset.
  Rerun: free flow 164,045, slow 35,825, congested 44,269, unmeasured 72,503.
- **Trade-off:** None. Periods are unique per section and bucket.
- **Lesson:** An enrichment that returns no match for one level is a bug until shown otherwise.
  The run record's per-level coverage made this visible in one look.

## 2026-11-09 — Failing loudly on an empty snapshot would have blocked a day for good

- **Area:** Silver
- **Symptom:** Review asked that a well-formed DATEX snapshot with no content fail the day, not
  publish it empty and ready. Implemented as a contract error, it failed 2026-11-03.
- **Evidence:** One weather poll at 19:21:36 UTC, 1 KB, with `returnStatus fail`, "Delivery break"
  and no payload. It was the only one in ~12,000 polls of all five feeds, 2026-10-28 to 11-08.
- **Cause:** NPRA reports its own delivery failures as valid messages. Bronze is immutable, so
  every rerun of that day would have failed the same way.
- **Fix:** That answer is recognised and quarantined as `source_delivery_failed` in
  [pipeline/silver_datex_parse.py](../../pipeline/silver_datex_parse.py). Like a `fetch_error`
  poll it does not block the day, because the next snapshot repeats the data. Any other empty
  snapshot is still `unknown_message_shape` and fails the day.
- **Lesson:** Run a new failure rule over every day of real input before merging it. A rule that
  fails a day must leave a way for that day to pass again.

## 2026-11-07 — A Spark bug lost join matches against cached data

- **Area:** Silver
- **Symptom:** In the first run of the Silver ablation (branch `experiments/silver-ablation`)
  only 19.96 % of test calls had a stop-level history. Yet 99.6 % of their keys (line, quay, hour,
  day type) exist in the profile days. The loss was the same in every hour, and computing the same
  features again gave 43.7 %.
- **Evidence:**

  - With the profile not cached, the join gave 99.65 % every time.
  - With the profile cached, it was right the first time, while the cache fills, and then gave
    43.69 %. All 200 sampled misses exist in the cache.
  - The plan shuffles the calls into 4 partitions and reads the cache with no shuffle at all.
  - With AQE off it gave 99.65 %.
  - With AQE on and `canChangeCachedPlanOutputPartitioning=false` it gave 99.65 % three times.

- **Cause:** Spark 3.5.0 bug SPARK-45282 ("Join loses records for cached datasets"). A cached plan
  is optimised by AQE. Its real partitioning differs from what the join assumes, so matching rows
  end up in different partitions. A left join hides it, because rows are not lost and their
  columns are null.
- **Fix:** `spark.sql.optimizer.canChangeCachedPlanOutputPartitioning=false` in `create_spark`
  ([pipeline/bronze.py](../../pipeline/bronze.py)), so every job has it.

  - Entur Silver caches nothing.
  - DATEX Silver caches the quays before its spatial joins. It was rebuilt for 2026-11-02 to
    11-04, and its quay links are identical.
  - The ablation cached its profile. Only it was wrong, and it was rerun.

- **Lesson:** A grain check (rows in = rows out) does not catch a left join that finds too few
  matches, so check coverage too. What made us look was that 19.96 % looked like the 20 % sample
  rate. That was a coincidence.

## 2026-11-07 — DATEX Silver hung after running out of heap

- **Area:** Silver
- **Symptom:** DATEX Silver for 2026-11-03 stayed at stage 4, `(5 + 1) / 6`, for 27 minutes with
  the JVM and Python idle, holding the Spark lock. 2026-11-02 had taken 128 s.
- **Evidence:** `java.lang.OutOfMemoryError: Java heap space` in the thread
  `stdout writer for python`, while decompressing a payload page
  (`HeapBytesDecompressor.decompress` under `VectorizedColumnReader.readPage`). The heap was 3 GB,
  with four tasks. The day's situation snapshots are up to 27.3 MB each, 2026-11-02's up to
  25.3 MB.
- **Cause:** Each task read 4 snapshots per Parquet batch into one column buffer that grows by
  doubling, then copied them into rows for Arrow. Four tasks hold most of the heap before a page is
  decompressed. When the thread that feeds Python dies, Spark does not fail the task, and Python
  waits for input forever.
- **Fix:** `columnarReaderBatchSize` 1 for the DATEX decode (`ONE_SNAPSHOT_AT_A_TIME` in
  [pipeline/silver_datex.py](../../pipeline/silver_datex.py)). The rerun took 107 s. There are
  about 285 snapshots a day, so batching bought nothing. The hung step was stopped by hand.
- **Lesson:** Size buffers for the largest row, not the average. A Spark job can also hang instead
  of fail, so a step that holds the lock needs someone watching the clock. The nightly run's
  `flock -w` only protects the waiter.

## 2026-11-06 — Dropping repeated ET states changed 650 predictions

- **Area:** Silver
- **Symptom:** The faster decoder (each UTC day once, only changed states) reproduced every stop
  call time of 2026-10-29 exactly. But 621 of 33 M `call_prediction` rows had another predicted
  time (median 10 s off) and 29 were missing.
- **Evidence:** Compared key by key with the published run.

  - Raw messages of one AKT call. The copy at 14:14:43.8 (recorded :33.5) says 15:39:41. So does
    the copy at 14:14:44.9 (recorded :34.5). The one at 14:14:45.0 is a replay recorded at :33.7
    that says 15:39:31.
  - One INN journey. Its 01:02 message was missing because the same task had read the 03:57 one
    first.
  - After a first fix 6 remained. A TRO call's 18:47 state was dropped as a repeat of its 15:xx
    state, because that task read a piece of hour 15 and then one starting at 18:47. The states in
    between were in other tasks.

- **Cause:** Two assumptions in "keep the first copy of a state":

  1. The as-of rule drops a late replay of older data because a newer copy was seen. The newer
     copy was the repeat we dropped.
  2. A task's rows are not one unbroken run of messages. Spark packs pieces of several hourly
     files into one task, largest first, so the input can jump back or forward over hours.

- **Fix:** The decoder remembers the newest copy of each entity even when it drops it, and drops a
  replay that is older and adds no actual time (`says_something_new` in
  [pipeline/silver_parse.py](../../pipeline/silver_parse.py)). Its memory starts over wherever
  consecutive rows are more than 60 s apart, either way. Messages are at most 9.5 s apart even at
  night, and arrival order jitters by at most 0.4 s (5,776 steps back in 400,802 lines).
- **Result (2026-10-29, key by key against the published run):** No prediction, call or journey is
  missing. Vehicles and SX are identical. 2 of 33.0 M predictions differ (3 s). 100 have a later
  `available_at` (4 to 26 s in a sample). All of them are at one task boundary, 15:52:13.
- **Trade-off:** Memory does not cross a task boundary. Before 15:52:13 a task dropped an AKT
  copy (recorded 15:52:02.4) as a repeat. The next task starts fresh, so the replay at 15:52:16.9
  (recorded 15:52:01.6) is kept. The as-of rule never saw the newer copy, so the replay counts.
  Carrying memory across tasks would need ordered input or a second pass, which is not worth 102
  rows.
- **Lesson:** An optimisation that drops rows must preserve every rule downstream that reads those
  rows, and a row order the engine never promised is not an order.

## 2026-11-05 — A long Silver run cost us a day of Bronze, and nothing said so

- **Area:** Bronze, CI/CD
- **Symptom:** Prod run 74 failed in **Run Bronze** after exactly 3 h with no Spark output. SIRI and
  vehicles Bronze for 2026-11-04 were never written. It was found a day later, in an audit, because
  the status page only watches the collectors and the next night's run only does its own day.
- **Evidence:**

  - GitHub Actions run `42010536942`. `flock -w 10800` gave up at 13:25 UTC.
  - A manual 4-hour Silver validation held `/data/dat535/.spark.lock` from 10:22 to 14:19.
  - Its runner reserved the "nightly slot" until 06:00 UTC, but the 03:00 cron has started at
    08:09-10:36 UTC since October.

- **Cause:** One lock, and one job holding it longer than the others wait. GitHub delays scheduled
  runs by hours, so a slot can't be reserved around them.
- **Fix:** Run 76 reran the day by hand. Run 77 reran 2026-10-28 for the DATEX that #35 left
  empty. Silver no longer takes 4 hours ([overview.md](overview.md), section 3), which removes the
  cause. Automatic catch-up and an alert were built and rejected (#39). Rejecting them kept less
  code to explain and left reruns manual.
- **Trade-off:** A lost day shows only in the failed run's email or the next audit. After long
  manual jobs, check with `ls /data/dat535/prod/bronze/siri-et | tail`.
- **Lesson:** Long manual runs on the shared lock are themselves an operational risk. Keep them
  away from the morning, and look at the batch after them.

## 2026-11-05 — A minimum-date SX timestamp overflowed during UTC conversion

- **Area:** Silver parser and full-feed validation.
- **Symptom:** The first complete-input 2026-10-29 pilot failed after 6 h 30 min while
  decoding SX, after finishing the large ET input. No Silver day or Gold output was published.
- **Evidence:** [Failed run record](evidence/silver-2026-10-29-baseline-failed.json),
  `local-20261104T181119Z-620f6795`. Two of 4,701 closed SX envelopes contain
  `StartTime=0001-01-01T00:00:00+00:53:28`. Python parses that offset, but conversion to
  UTC would precede year 1 and raises `OverflowError`.
- **Cause:** `timestamp()` in [pipeline/silver_parse.py](../../pipeline/silver_parse.py) handled
  invalid syntax and missing offsets, but omitted this boundary case, where the input is
  representable and its UTC value is not.
- **Fix:**

  - Handle `OverflowError` with the existing invalid-timestamp policy: preserve the source
    record, set that typed value to null, and add `invalid_validity_start`.
  - The original XML remains unchanged in landing and traceable through Bronze, and its content
    hash remains intact.
  - Decode the small SX/vehicle feeds before nationwide ET to expose decoder exceptions earlier.

- **Validation:** The regression includes the actual offset and both UTC range boundaries.
  A read-only pass over all 4,701 real SX envelopes produced 5,947 situation versions and
  1,439 controls. Spark's row verifier/converter accepted every row, with exactly two
  `invalid_validity_start` flags. Full retry validation is tracked in the
  [week evidence](evidence/silver-week-2026-10-26-to-2026-11-01.md).
- **Trade-off / lesson:** A timestamp with a legal ISO offset can still be outside the
  target type after standardisation. Exercise the full small feeds before a long ET run.

## 2026-11-04 — Gold must discover dates from Silver partition paths

- **Area:** Gold, Silver consumer contract.
- **Symptom:** The first VM scorecard run failed with `UNRESOLVED_COLUMN` for
  `operating_date` while reading the saved national line reference table.
- **Evidence:** The 2026-10-26 Silver sample stores that reference date in
  `line/operating_date=2026-10-26/`, without repeating it in the Parquet columns. The first
  fixture tests passed because their files did repeat the date.
- **Cause:** Gold read a single partition directory without declaring the table root, so
  Spark did not discover the partition key. Fact files and reference files need not have
  the same physical representation of a partition column.
- **Fix:** Declare `basePath` for each Silver table read in
  [pipeline/gold_scorecard.py](../../pipeline/gold.py) (now `gold.py`), so Spark recovers the partition
  date. The regression fixture now omits the physical date column. No Silver files were changed.
- **Validation:** All 5 Gold tests passed. The VM run reconciled 76,186 journeys and
  1,855,992 calls into 3,953 line/day rows. See the
  [example run record](evidence/gold-2026-10-26-sample.json).
- **Trade-off / lesson:** A partition key is part of the dataset contract, even when it is
  absent from individual files. Test consumers against the physical files as well as
  convenient in-memory fixtures.

## 2026-10-29 — DATEX Bronze ran out of heap on its first full day

- **Area:** Bronze, VM
- **Symptom:** The scheduled prod run failed in **Run Bronze DATEX** after ~35 s with
  `java.lang.OutOfMemoryError: Java heap space` while writing `datex-situation/date=2026-10-28`.
  SIRI and vehicles Bronze for the same day had succeeded (issue #35).
- **Evidence:**

  - GitHub Actions run `40849746177` (prod, schedule). Parquet logged
    `InternalParquetRecordWriter: Too much memory used`.
  - Every situation line is one snapshot of ~26.6M characters, twelve per hour file.
  - 2026-10-28 had 13 hour files, because DATEX was deployed at 11:30 UTC. They are 339 MB
    gzipped, and the job coalesces them into 3 write tasks of ~50 snapshots each.
  - The same code on the same day on the VM failed again after 37 s.

- **Cause:** Parquet first measures how much a writer has buffered after
  `parquet.page.size.row.check.min` = 100 rows, for pages and row groups alike. A task with
  fewer rows than that keeps all of them in one page until it closes. That is ~1.3 GB of snapshots
  per task, three tasks at once, plus the JSON parser's copies of each line, against a 4 GB heap.
  The dev workflow only ever ingests one hour (12 snapshots), so this first showed on the first
  full day in prod.
- **Fix:** The DATEX write in [pipeline/bronze_datex.py](../../pipeline/bronze_datex.py) sets
  `parquet.page.size.row.check.min` to 1, so the writer measures from the first row and writes a
  page per snapshot. On the VM the same day took 55 s with the 4 GB heap, and it also passes with
  2 GB. All five feeds ran in 60 s with no bad rows.
- **Trade-off:** The writer measures its buffers after every row. With ~150–300 rows per feed
  and day that costs nothing. [`bronze.py`](../../pipeline/bronze.py) keeps the default, because
  ET rows are mostly small and it has run daily without this problem.
- **Lesson:** Parquet's memory control counts rows before it counts bytes, so a column of
  uniformly huge values defeats it. Run a new source's Bronze on a full real day before it is
  scheduled, not only the dev workflow's single hour.

## 2026-10-28 — Missing and malformed real-time data crossed Silver contract boundaries

- **Area:** Silver, readiness and regression coverage.
- **Evidence:** The review of PR #34 found these, and each was reproduced before the fix:

  - 120 corrupt envelopes spanning every required hour published `ready=true`.
  - A vehicle's available dated alias left its operating date null.
  - A known cancellation with no call history produced no sample.
  - A GraphQL error-array payload raised `AttributeError`.

- **Cause:** Hour presence was treated as sufficient coverage without gating all source
  contract failures. Journey lookup and prediction sampling assumed a particular ID/call
  shape, while the vehicle decoder assumed every transport payload was an object.
- **Fix:** See the Silver tests
  ([pipeline/tests/test_silver.py](../../pipeline/tests/test_silver.py)).

  - Gate all Bronze envelope/vehicle shape failures.
  - Resolve explicit dated aliases.
  - Preserve cancellation targets without call versions and retain their journey evidence.
  - Decode transport errors before accessing data objects.

- **Validation:** Offline regressions cover strict/partial runs with all hours present,
  preservation of the previous marker, both ID forms, cancellation before any call update,
  cutoff timing, error arrays and unexpected envelope shapes.
- **Trade-off / lesson:** Strict source-contract failures require repair or an explicit
  exploratory run, even if only one envelope is affected. Field-level quality flags remain
  nonfatal. Test combined missing/error states rather than only valid messages and isolated
  parser fields.

## 2026-10-28 — National NeTEx references need scope and native versions

- **Area:** Silver, reference data.
- **Symptom:** A parser keyed only by calendar-assignment ID rejected FIN. Choosing shared
  calendars from filenames left SKY/RUT references unresolved. An operator join also found
  multiple versions of the same ID in the national archive.
- **Evidence:** `netex/date=2026-10-26`:

  - FIN assignment `Cal2026102644412-Jan_Fri_01-X` repeats for different `DayTypeRef` values.
  - `SKY_arbergsdalen_sande.xml` contains a `FlexibleLine`.
  - VIL/VYX fixed lines reference definitions in flexible shared files.
  - `KOL:Operator:312` appears at versions 6 and 1551.

- **Fix:** Preserve assignment selectors, inspect the XML line type, resolve fallback date
  rules in their own shared-file scope, and select the highest numeric reference version.
  Conflicting values at the same version still fail before a fact join.
- **Validation:** The complete national plan produced 76,186 journeys and 1,855,992 calls.
  The 1,879 missing quay mappings remained flagged. Offline regressions cover each case.
  Run record: [Silver sample evidence](evidence/silver-2026-10-26-sample.json).
- **Trade-off / lesson:** Two-pass line parsing and scoped dictionaries cost extra XML
  reads, but avoid deleting valid services or silently multiplying facts. Native IDs alone
  do not always identify an exported child relation.

## 2026-10-24 — Bronze Parquet was 2.6× larger than the gzipped input

- **Area:** Bronze, VM
- **Symptom:** A day of ET took ~8.5 GB in landing (gzip) but ~22 GB in prod Bronze. At that
  rate the 984 GB data volume would have been full by mid-November.
- **Evidence:** `du` on `/data/dat535/prod/bronze/siri-et/date=2026-10-19..22` showed
  19.9–24.1 GB per day. One hour (`siri-et/date=2026-10-22/hour=08`, 589 MB gzipped) was written
  three ways on the VM:

  - snappy gave 1,621 MB in 126 s.
  - gzip gave 626 MB.
  - zstd gave 558 MB in 129 s.

- **Cause:** Bronze stores each push as one string of up to ~16M characters. Snappy is built for
  speed and barely compresses large XML, while zstd finds the repetition.
- **Fix:** `spark.sql.parquet.compression.codec=zstd` in `create_spark`
  ([pipeline/bronze.py](../../pipeline/bronze.py)).
- **Trade-off:** None measured. The write time is the same, and a day takes ~14 GB less.
- **Lesson:** A default that is right for typical tables with small columns can be wrong for ours
  with huge strings. Measure the layer's size against its input.

## 2026-10-24 — Reading Bronze payloads killed the Spark driver

- **Area:** Bronze, Silver, VM
- **Symptom:** Summing `length(payload)` over one hour of Bronze ET ended with the driver JVM gone
  (`Java heap space`, then `Connection refused` from py4j).
- **Evidence:** Same hour as in the entry above, `local[4]`, `spark.driver.memory=3g`.

  - The default `columnarReaderBatchSize` (4096) crashed.
  - 256 took 23 s and 64 took 21 s.
  - The hour holds 207,380 pushes and 10.8 G characters. The largest push is 15.9 M.

- **Cause:** The vectorized Parquet reader decodes 4096 rows per batch. With multi-MB strings
  and four tasks at once, the batches alone exceed the heap.
- **Fix:** `spark.sql.parquet.columnarReaderBatchSize=256` in `create_spark`
  ([pipeline/bronze.py](../../pipeline/bronze.py)), so Silver inherits it.
- **Trade-off:** None measurable at this size.
- **Lesson:** Silver will read every payload, so size reader batches by bytes, not rows.

## 2026-10-24 — The largest ET pushes were marked corrupt

- **Area:** Bronze
- **Symptom:** Every prod Bronze run reported 1–2 `corrupt` ET rows.
- **Evidence:** The two on 2026-10-22 (hour 21) had raw lines of 21,557,914 and 22,773,516
  characters. In a test, Spark 3.5.0 read a 19,999,000-character string and marked a
  20,000,100-character one corrupt.
- **Cause:** Spark 3.5's JSON reader uses Jackson 2.15, which caps strings at 20,000,000
  characters by default. Overriding Jackson's global default did nothing, because Spark builds its
  own constraints from the reader option `maxStringLen`.
- **Fix:** `.option("maxStringLen", 2**31 - 1)` on every JSON read of landing in
  [pipeline/bronze.py](../../pipeline/bronze.py) and
  [pipeline/bronze_datex.py](../../pipeline/bronze_datex.py), with a regression test. A DATEX
  situation snapshot (~28.6 M characters) would otherwise always be corrupt.
- **Trade-off:** None. Rerun old days to reclassify their rows.
- **Lesson:** "corrupt" rows deserve a look even when there are only two. They were the
  biggest messages, likely full snapshots after a re-subscribe.

## 2026-10-24 — Bronze ran out of disk on the root volume

- **Area:** Bronze, VM
- **Symptom:** The scheduled prod Bronze run failed after ~3.5 minutes with
  `java.io.IOException: No space left on device`.
- **Evidence:**

  - GitHub Actions run `40076009618` (prod, schedule).
  - Spark log:
    `SparkOutOfMemoryError: error while calling spill() on UnsafeExternalSorter : No space left on device`.
  - VM disks: root `/dev/vda1` 38 GB (81 % used, 7.4 GB free), data volume `/data` 984 GB.
  - A leftover `/tmp/blockmgr-*` from an earlier crashed run held 988 MB.

- **Cause:** `df.write.partitionBy("date")` makes Spark sort rows by the partition column
  before writing, even though `date` was a single constant per run. With ~200 GB of ET lines
  per day the sort spills to `spark.local.dir`, which defaults to `/tmp` on the small root disk.
- **Fix:** Bronze writes each day straight into `siri-<feed>/date=<day>` with overwrite
  instead of using `partitionBy`. No sort, no spill. Readers of the feed root still get `date`
  through partition discovery. Moving `spark.local.dir` to `/data` was rejected. `/data` is a
  network-mounted volume, so spilling there trades a full-disk failure for slow network I/O, and
  the spill would still cost a full extra write of the day.
- **Trade-off:** Lost atomic replacement of a day. Overwriting a path deletes it before the
  write starts, so a failed rerun leaves the day missing until it is rerun. Bronze can always
  be rebuilt from the immutable landing zone. With `partitionBy` and dynamic overwrite the old
  partition survived a failed run.
- **Lesson:** Spark operations that look free, such as `partitionBy` on a constant, can trigger
  hidden sorts and shuffles. Check the physical plan and where spills land. Silver will need real
  shuffles, so local spill space must be planned for, not left at the default.

## 2026-10-23 — DATEX did not behave as its specification suggested

- **Area:** collector (DATEX)
- **Symptom:** The poller was designed around conditional GET (`If-Modified-Since`, 304) and
  gzip, as NPRA's spec describes for "some" services.
- **Evidence:** One GET per feed from the VM showed no `Last-Modified` and no `Content-Encoding`
  on any of the five feeds. A situation snapshot is 28.6 MB. Two GETs 20 s apart returned
  byte-identical travel time and weather snapshots.
- **Cause:** The spec is generic, and our services support neither.
- **Fix:** Fixed intervals per feed, set from measured sizes and NPRA's cadence. Change is
  detected by hashing the body, where identical bytes mean no change. A fake-server test also
  found that `http.client`'s `read(n)` returns short instead of raising when a server closes
  early, so the poller ([collector/datex.py](../../collector/datex.py)) now checks
  `Content-Length` itself.
- **Trade-off:** Every poll downloads the full snapshot. That is ~8.8 GB/day, nearly all of it
  situations.
- **Lesson:** Verify a source against real responses before designing around its documentation.
