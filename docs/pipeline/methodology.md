# Methodology

Bronze preserves evidence, Silver defines reusable facts, and Gold answers a consumer's question.
This page records the decision rules. `✓` means implemented and `→` means planned.

Up: [overview](../report/overview.md) · Related: [Silver](silver.md), [Gold](gold.md),
[storage](../system/storage.md)

## General

- Start with daily batches. Require a named consumer before adding fresher processing.
- Design failure and manual reruns before tuning speed. Keep landing immutable.
- Profile before tuning. Support changes with row counts, timings, sizes or a Spark profile.
- Fit the VM's 4 vCPU, 7.9 GB RAM and 38 GB root disk. Follow the
  [storage rules](../system/storage.md#memory).

## Bronze: load first, interpret later

| Rule | Application |
| ---- | ----------- |
| Preserve source evidence | ✓ Store XML/JSON payloads unchanged, plus receipt time, file, hash, size, status and run ID. |
| Keep unreadable input | ✓ Retain corrupt lines and their raw text. Raise the JSON string limit for large payloads. |
| Measure delivery | ✓ Report missing/open hours and DATEX poll outcomes, distinct snapshots and longest gap. |
| Keep recovery manual | ✓ Replace only the requested output date. Never modify landing. |
| Understand the source contract | ✓ Document field inventories. → Detect new or vanished XML elements automatically. |

The implementations are [Entur Bronze](../../pipeline/bronze.py) and
[DATEX Bronze](../../pipeline/bronze_datex.py). Hour coverage does not prove that every source
message arrived. Planned-versus-reported coverage belongs to
[Silver](silver.md#planned-journeys-unseen-in-et-2026-10-23).
Manual cleanup removes rebuildable data before raw history, following the
[storage order](../system/storage.md#when-it-fills-delete-by-hand-cheapest-loss-first).

## Silver: where trust is built

Put facts about one record in Silver and consumer choices in Gold. If several products need
the same definition, it belongs in Silver. Actual minus aimed time belongs in Silver, while
a three-minute on-time threshold belongs in Gold.

| Check | Decision rule |
| ----- | ------------- |
| Shape | Use explicit schemas and typed UTC timestamps. Make unfamiliar structures visible. |
| Identity | Define the grain and key before transforming. Reduce versions before joining. |
| Trust | Retain field flags on rows. Quarantine unusable identities or messages. Block publication on contract failures. |
| Conformity | Check IDs against reference snapshots. Keep IDs in facts and add labels in Gold. |
| Time | Separate operating dates from UTC observation dates. Replace only the requested partitions. |

Final ET calls have one row per journey and stop position on an operating day. Sampled
predictions have their own grain. SX and road situations retain revisions, and decoded
versions remain in a rebuildable cache. See the [Silver contracts](silver.md#tables).

Use facts and reference tables rather than Data Vault for these few, event-heavy sources.
Record input, output and quarantine counts by reason. Review quarantine rates across days
for source drift.

## Gold: serve a use, not a layer

- Name the consumer and materialise only what it uses. Delivered Gold is retrospective: a line
  scorecard, a stop map, observed transfers and late departures under road conditions
  ([what is built](gold-example.md)). Departure-risk prediction is [planned](gold.md#1-departure-risk-primary).
- Store additive counts, so consumers sum days. A rate or percentile averaged over days is wrong.
- Aggregate finer facts rather than joining grains. Define thresholds, cancellation semantics
  and denominators once. Missing real-time is neither punctuality nor cancellation.
- ✓ Gold checks both Silver markers, reconciles every table's departures to the scorecard
  and publishes a run marker.
- → Cross-check the same day against Entur's BigQuery. Access and contents remain
  **UNVERIFIED**. See [Entur sources](../entur/sources.md#bigquery-history-reference).
- → Validate a prediction model with time-based splits, a simple baseline and calibration.

## Spark layout

- Partition by the date consumers filter on. Bronze uses UTC `date`. Silver uses
  `operating_date` or UTC `observation_date`, and Gold uses `operating_date`.
- Parse and project before shuffles. Broadcast small references. Check skew and join coverage,
  especially because RUT and SKY dominate calls.
- Size files from the input where useful, as DATEX Bronze does. Avoid sorting on a constant
  partition column.
- Keep one Spark job at a time and spill on `/data`. The
  [storage guide](../system/storage.md#memory) owns settings and their measurements.
  Its [compression comparison](../system/storage.md#what-a-day-costs) explains the zstd choice.
