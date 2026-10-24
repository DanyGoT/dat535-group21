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
