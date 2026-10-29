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
