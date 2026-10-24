# Methodology

The decision rules we apply layer by layer: Bronze keeps evidence, Silver builds trust, Gold
serves a purpose. `✓` = in place, `→` = to do. Specifics live in [pipeline/silver.md](silver.md), [pipeline/gold.md](gold.md) and
[system/storage.md](../system/storage.md); this page is the "why".

## General

- **Batch first.** One run per operating day is enough. Go fresher only when a named
  consumer acts on fresher data.
- **Ask what breaks at 2 AM before asking what is fast.** Every job is safe to rerun and
  fails loudly.
- **Profile before tuning.** Every optimisation comes with its evidence (Spark UI, row
  counts, timings, sizes). The report needs it. Example: Bronze switched to zstd after one
  real hour measured 1,621 MB as snappy and 558 MB as zstd in the same write time.
- **Fit the machine.** One 8 GB VM with a 38 GB root disk: memory and spill space are design
  constraints, not afterthoughts ([system/storage.md](../system/storage.md)).

## Bronze: load first, interpret later

| Principle | Here |
| --------- | ---- |
| Keep the payload as it arrived | ✓ collectors don't parse; raw XML/JSON per line, unchanged in Bronze |
| Payload plus evidence | ✓ `received_at`, source file, payload hash and size, status, run id |
| Prove complete delivery (a green run isn't proof) | ✓ missing and open hours per run; DATEX: polls by outcome, distinct snapshots, longest gap. → expected vs received journeys (needs the archived plan) |
| Never lose a line | ✓ unreadable lines kept with `_bronze_status` and the raw line; payloads > 20M characters read in full (`maxStringLen`) |
| Retain for recovery | ✓ landing is never modified; a rerun replaces only its day; retention deletes rebuildable Bronze before any raw data |
| Contract with the source | ✓ field inventories in [entur/](../entur/) and [datex/content.md](../datex/content.md). → detect schema drift (a new or vanished element) |

## Silver: where trust is built

**Boundary test:** facts about one record go in Silver, answers to questions go in Gold.
Unsure? If two or more Gold products need it with the same definition, it is Silver.
- `delay_s = actual − aimed` per call: Silver.
- "On time = less than 3 min late": a business choice, so Gold.

**Five checks.** Each answers a question about every row:

| | Question | Here |
| - | -------- | ---- |
| Shape | Is it readable? | explicit schema, typed UTC timestamps, flattened calls; unknown structure fails loudly |
| Identity | Is it one record, once? | key from the grain, latest version kept ([pipeline/silver.md](silver.md) §5) |
| Trust | Can we rely on it? | rules flag rows, never delete them; failures go to quarantine with a `dq_issue` reason |
| Conformity | One vocabulary? | ids checked against NSR/NeTEx reference tables; ids in facts, labels in Gold |
| Time | Can we rerun it safely? | per operating day, overwrite that partition, same input gives same output |

**Grain:** start from the finest grain people still understand, the stop call. Don't keep the
version grain (~57 per call) in Silver; Bronze has it. The one exception is a sampled
prediction table at fixed lead times (`call_prediction`), which is a grain of its own.

**Model:** facts plus reference tables, not Data Vault: few sources, event-heavy data.

**Operate it:** per run, rows in, rows out and rows quarantined by reason. Track the
quarantine rate over days: a jump means the source changed.

## Gold: serve a use, not a layer

- **No named consumer, no Gold table.** Ours: travellers asking "will my departure be on
  time?" ([pipeline/gold.md](gold.md) #1), plus independent products on the same Silver.
- **Define the semantics once:** on-time threshold, what counts as cancelled, and the
  denominator (planned vs reported, [pipeline/silver.md](silver.md) §4).
- **Materialise only what is used.** Anything else is a view or a query over Silver.
- **Ready signal and quality gate:** publish only after the checks pass, and validate
  aggregates against Silver totals and against Entur's BigQuery for the same day. The
  consumer reads a marker (the run record), not "the file exists".
- **Validate predictions honestly:** split by time, compare with a simple baseline, report
  calibration.

## Spark layout

- Partition by the column queries filter on: `operating_date` (Silver, Gold) or the UTC
  `date` (Bronze). Avoid many small files: DATEX Bronze sizes its output from its input.
- Narrow early: parse and project before shuffles. A raw ET day is ~9 GB gzipped, ~160 GB
  of characters.
- Broadcast small dimensions (stops, lines). Watch for skew: RUT and SKY dominate calls.
- Settings with evidence behind them (zstd, reader batch 256, `maxStringLen`, no
  `partitionBy` on constants, one job at a time): [system/storage.md](../system/storage.md).
