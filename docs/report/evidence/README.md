# Evidence

Run records and reports from real VM executions support the numbers cited in the docs.
The week log records failures, fixes and resource measurements.

Up: [system overview](../overview.md) · Related: [problem log](../problems.md),
[Silver](../../pipeline/silver.md#evidence-and-measurements),
[Gold](../../pipeline/gold-example.md#validated)

Except for the 2026-11-05 to 11-09 Gold runs, these are historical artifacts, not validation of
the current code revision. The October Silver/Gold runs use schema 1 and predate today's
implausible-delay exclusion.

## Gold with road data, 2026-11-05, 08 and 09

Run on 2026-11-11 with the current [`gold.py`](../../../pipeline/gold.py) against prod Silver
(Entur schema 2 and DATEX, all `complete, ready=true`), written to a separate output root.

| File | Shows |
| ---- | ----- |
| [`gold-2026-11-05.json`](gold-2026-11-05.json), [`-08`](gold-2026-11-08.json), [`-09`](gold-2026-11-09.json) | Run records: Silver and DATEX run IDs, four table sizes, totals, condition coverage, runtime |
| [`gold-dashboard-2026-11-transfers.png`](gold-dashboard-2026-11-transfers.png) | Dashboard over the three days: transfers from bus 42 at Lysaker |
| [`gold-dashboard-2026-11-roads.png`](gold-dashboard-2026-11-roads.png) | Late departures under road conditions, matched on mode and hour |
| [`gold-dashboard-2026-11-mobile.png`](gold-dashboard-2026-11-mobile.png) | A transfer's chance by spare minutes and hour at 390 px |

## Complete day, 2026-10-29

These records come from the first scorecard job, before it became [`gold.py`](../../../pipeline/gold.py).
This operating day was validated end to end on 2026-11-05.
Both publication records say `complete`, `ready=true`.

| File | Shows |
| ---- | ----- |
| [`silver-2026-10-29.json`](silver-2026-10-29.json) | Input coverage, table counts, issues and runtime |
| [`gold-2026-10-29.json`](gold-2026-10-29.json) | Gold totals reconciled against that Silver run |
| [`gold-line-scorecard-2026-10-29.md`](gold-line-scorecard-2026-10-29.md) | Generated scorecard report, top 25 reported lines |
| [`gold-dashboard-2026-10-29.png`](gold-dashboard-2026-10-29.png), [mobile](gold-dashboard-2026-10-29-mobile.png) | The first scorecard viewer showing that day |

## Bounded sample, 2026-10-26

The first sample used the full national plan and a few real-time messages.
Both archived records say `partial`, `ready=false`.
These sample artifacts do not describe the current prod marker for that date.

| File | Shows |
| ---- | ----- |
| [`silver-2026-10-26-sample.json`](silver-2026-10-26-sample.json) | Silver run record of the sample |
| [`gold-2026-10-26-sample.json`](gold-2026-10-26-sample.json) | Gold run record of the sample |
| [`gold-line-scorecard-2026-10-26-sample.md`](gold-line-scorecard-2026-10-26-sample.md) | Generated sample scorecard |

## Getting a full day through Silver

The one-off validation did not complete all seven requested operating days.

| File | Shows |
| ---- | ----- |
| [`silver-week-2026-10-26-to-2026-11-01.md`](silver-week-2026-10-26-to-2026-11-01.md) | Historical full-day validation log |
| [`silver-2026-10-29-baseline-failed.json`](silver-2026-10-29-baseline-failed.json) | Failed first full-day attempt |
| [`silver-et-decode-2026-10-29.json`](silver-et-decode-2026-10-29.json) | Unscoped three-day ET decoding measurements |
| [`silver-et-decode-scoped-2026-10-29.json`](silver-et-decode-scoped-2026-10-29.json) | ET decoding after early filtering of other operating days |
| [`silver-et-scope-equivalence-2026-11-05.json`](silver-et-scope-equivalence-2026-11-05.json) | Early-filter equivalence check |
| [`silver-decode-cpu-profile-2026-11-04.json`](silver-decode-cpu-profile-2026-11-04.json) | Small CPU profile of decoder steps |
