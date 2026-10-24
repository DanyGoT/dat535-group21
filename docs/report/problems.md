# Problem log

Problems we ran into while building the pipeline, kept as raw material for the report's
reflection and lessons learned (`PROJECT.md` §7). Newest first. One entry per problem; keep the
evidence (run IDs, log lines, numbers) so claims in the report can be backed up.

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

## 2026-10-24 — Bronze Parquet was 2.6× larger than the gzipped input

- **Area:** Bronze, VM
- **Symptom:** A day of ET took ~8.5 GB in landing (gzip) but ~22 GB in prod Bronze. At that
  rate the 984 GB data volume would have been full by mid-November.
- **Evidence:** `du` on `/data/dat535/prod/bronze/siri-et/date=2026-10-19..22`: 19.9–24.1 GB per
  day. One hour (`siri-et/date=2026-10-22/hour=08`, 589 MB gzipped) written three ways on the VM:
  snappy 1,621 MB in 126 s, gzip 626 MB, zstd 558 MB in 129 s.
- **Cause:** Bronze stores each push as one string of up to ~16M characters. Snappy is built for
  speed and barely compresses large XML; zstd finds the repetition.
- **Fix:** `spark.sql.parquet.compression.codec=zstd` in `create_spark`.
- **Trade-off:** none measured: same write time, ~14 GB/day less.
- **Lesson:** a default that is right for typical tables (small columns) can be wrong for ours
  (huge strings). Measure the layer's size against its input.

## 2026-10-24 — Reading Bronze payloads killed the Spark driver

- **Area:** Bronze, Silver, VM
- **Symptom:** Summing `length(payload)` over one hour of Bronze ET ended with the driver JVM gone
  (`Java heap space`, then `Connection refused` from py4j).
- **Evidence:** Same hour as above, `local[4]`, `spark.driver.memory=3g`. Default
  `columnarReaderBatchSize` (4096): crashed. 256: 23 s; 64: 21 s. The hour holds 207,380 pushes,
  10.8 G characters, the largest 15.9 M.
- **Cause:** The vectorized Parquet reader decodes 4096 rows per batch. With multi-MB strings
  and four tasks at once, the batches alone exceed the heap.
- **Fix:** `spark.sql.parquet.columnarReaderBatchSize=256` in `create_spark`, so Silver inherits it.
- **Trade-off:** none measurable at this size.
- **Lesson:** Silver will read every payload; size reader batches by bytes, not rows.

## 2026-10-24 — The largest ET pushes were marked corrupt

- **Area:** Bronze
- **Symptom:** Every prod Bronze run reported 1–2 `corrupt` ET rows.
- **Evidence:** The two on 2026-10-22 (hour 21) had raw lines of 21,557,914 and 22,773,516
  characters. Locally, Spark 3.5.0 read a 19,999,000-character string and marked a
  20,000,100-character one corrupt.
- **Cause:** Spark 3.5's JSON reader uses Jackson 2.15, which caps strings at 20,000,000
  characters by default. Overriding Jackson's global default did nothing; Spark builds its own
  constraints from the reader option `maxStringLen`.
- **Fix:** `.option("maxStringLen", 2**31 - 1)` on every JSON read of landing, with a regression
  test. A DATEX situation snapshot (~28.6 M characters) would otherwise always be corrupt.
- **Trade-off:** none. Rerun old days to reclassify their rows.
- **Lesson:** "corrupt" rows deserve a look even when there are only two: they were the
  biggest messages, likely full snapshots after a re-subscribe.

## 2026-10-24 — Bronze ran out of disk on the root volume

- **Area:** Bronze, VM
- **Symptom:** The scheduled prod Bronze run failed after ~3.5 minutes with
  `java.io.IOException: No space left on device`.
- **Evidence:** GitHub Actions run `40076009618` (prod, schedule). Spark log:
  `SparkOutOfMemoryError: error while calling spill() on UnsafeExternalSorter : No space left on device`.
  VM disks: root `/dev/vda1` 38 GB (81 % used, 7.4 GB free); data volume `/data` 984 GB.
  A leftover `/tmp/blockmgr-*` from an earlier crashed run held 988 MB.
- **Cause:** `df.write.partitionBy("date")` makes Spark sort rows by the partition column
  before writing, even though `date` was a single constant per run. With ~200 GB of ET lines
  per day the sort spills to `spark.local.dir`, which defaults to `/tmp` on the small root disk.
- **Fix:** Bronze writes each day straight into `siri-<feed>/date=<day>` with overwrite
  instead of using `partitionBy`. No sort, no spill. Readers of the feed root still get `date`
  through partition discovery. Moving `spark.local.dir` to `/data` was rejected: `/data` is a
  network-mounted volume, so spilling there trades a full-disk failure for slow network I/O, and
  the spill would still cost a full extra write of the day.
- **Trade-off:** Lost atomic replacement of a day. Overwriting a path deletes it before the
  write starts, so a failed rerun leaves the day missing until it is rerun (Bronze can always
  be rebuilt from the immutable landing zone). With `partitionBy` and dynamic overwrite the old
  partition survived a failed run.
- **Lesson:** Spark operations that look free (`partitionBy` on a constant) can trigger hidden
  sorts and shuffles; check the physical plan and where spills land. Silver will need real
  shuffles, so local spill space must be planned for, not left at the default.

## 2026-10-29 — DATEX Bronze ran out of heap on its first full day

- **Area:** Bronze, VM
- **Symptom:** The scheduled prod run failed in **Run Bronze DATEX** after ~35 s with
  `java.lang.OutOfMemoryError: Java heap space` while writing `datex-situation/date=2026-10-28`.
  SIRI and vehicles Bronze for the same day had succeeded (issue #35).
- **Evidence:** GitHub Actions run `40849746177` (prod, schedule). Parquet logged
  `InternalParquetRecordWriter: Too much memory used`. Every situation line is one snapshot of
  ~26.6M characters, twelve per hour file; 2026-10-28 had 13 hour files (DATEX was deployed at
  11:30 UTC), 339 MB gzipped, which the job coalesces into 3 write tasks of ~50 snapshots each.
  The same code on the same day on the VM failed again after 37 s.
- **Cause:** Parquet first measures how much a writer has buffered after
  `parquet.page.size.row.check.min` = 100 rows, for pages and row groups alike. A task with
  fewer rows than that keeps all of them in one page until it closes: ~1.3 GB of snapshots per
  task, three tasks at once, plus the JSON parser's copies of each line, against a 4 GB heap.
  The dev workflow only ever ingests one hour (12 snapshots), so this first showed on the first
  full day in prod.
- **Fix:** The DATEX write sets `parquet.page.size.row.check.min` to 1, so the writer measures
  from the first row and writes a page per snapshot. On the VM, the same day: 55 s with the
  4 GB heap, and it also passes with 2 GB; all five feeds in 60 s, no bad rows.
- **Trade-off:** The writer measures its buffers after every row. With ~150–300 rows per feed
  and day that costs nothing. `bronze.py` keeps the default: ET rows are mostly small, and it
  has run daily without this problem.
- **Lesson:** Parquet's memory control counts rows before it counts bytes, so a column of
  uniformly huge values defeats it. Run a new source's Bronze on a full real day before it is
  scheduled, not only the dev workflow's single hour.
## 2026-10-23 — DATEX did not behave as its specification suggested

- **Area:** collector (DATEX)
- **Symptom:** The poller was designed around conditional GET (`If-Modified-Since`, 304) and
  gzip, as NPRA's spec describes for "some" services.
- **Evidence:** One GET per feed from the VM: no `Last-Modified` and no `Content-Encoding` on any
  of the five feeds; a situation snapshot is 28.6 MB. Two GETs 20 s apart returned
  byte-identical travel time and weather snapshots.
- **Cause:** The spec is generic; our services support neither.
- **Fix:** Fixed intervals per feed from measured sizes and NPRA's cadence; change detected by
  hashing the body (identical bytes = no change). A fake-server test also found that
  `http.client`'s `read(n)` returns short instead of raising when a server closes early, so
  the poller now checks `Content-Length` itself.
- **Trade-off:** every poll downloads the full snapshot: ~8.8 GB/day, nearly all situations.
- **Lesson:** verify a source against real responses before designing around its documentation.
