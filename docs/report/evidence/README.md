# Evidence

Run records and reports from real VM executions support the numbers cited in the docs.
The week log records failures, fixes and resource measurements.

Up: [system overview](../overview.md) · Related: [problem log](../problems.md),
[Silver](../../pipeline/silver.md#evidence-and-measurements),
[Gold](../../pipeline/gold-example.md#validated)

The November 2026 scheduled prod records and Silver quality report reflect the current schema-2
Entur Silver and schema-1 DATEX Silver code. Earlier October records are kept as historical
before/after evidence (schema 1, prior to operating-date derivation and implausible-delay flags).

## Scheduled prod runs and Silver quality, 2026-11

Extracted from scheduled GitHub Actions prod artifacts (`prod-bronze-runs-143`,
`prod-silver-runs-143`, workflow run `45402302399`).

| File | Shows |
| ---- | ----- |
| [`silver-quality-2026-11.md`](silver-quality-2026-11.md) | 20-day Entur Silver funnel, plan coverage, quarantine drop (4.04M → 0–3/day), `dq_issue` totals, top `actual == aimed` operators, and 19-day DATEX quality (including `depositionDepth` schema drift) |
| [`bronze-2026-11-20.json`](bronze-2026-11-20.json), [`bronze-datex-2026-11-20.json`](bronze-datex-2026-11-20.json) | Complete weekday Bronze runs (`gh-44895514041-1`): 4.93M ET, 3.2k SX, 2.11M vehicles, 0 bad rows, all 5 DATEX feeds |
| [`silver-2026-11-20.json`](silver-2026-11-20.json) | Complete schema-2 Entur Silver (`gh-45014021086-1`): 1.88M stop calls, 96.86 % reporting share, 0 quarantined, 1h 2m |
| [`silver-datex-2026-11-20.json`](silver-datex-2026-11-20.json) | Complete schema-1 DATEX Silver (`gh-44895514041-1`): 2,931 situations, 65,935 weather rows, 58,176 travel-time periods, 0 quarantined, 2m 05s |

## Rush-hour ingest, 2026-11-12 and 15

Run on 2026-11-20 with [`rush_hour.py`](rush_hour.py) against prod Bronze and Silver, plus the
panel's Prometheus history. Read-only, outside the pipeline.

| File | Shows |
| ---- | ----- |
| [`rush-hour-ingest-2026-11.md`](rush-hour-ingest-2026-11.md) | Why the ingest rate peaks: running journeys, restatements per journey, bytes each |
| [`rush-hour-2026-11-12.json`](rush-hour-2026-11-12.json), [`-15`](rush-hour-2026-11-15.json) | Per hour and codespace: ET messages, journey restatements, calls, bytes, planned journeys; vehicle reports |
| [`rush-hour-panel-2026-10-16-to-11-19.json`](rush-hour-panel-2026-10-16-to-11-19.json) | Uncompressed bytes landed per hour and feed, from Prometheus |

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
