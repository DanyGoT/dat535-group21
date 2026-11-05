# Gold line scorecard viewer

[Open the private frontend](https://dat535-gold-scorecard.ratedbiscuit.chatgpt.site).

A small static frontend over the existing Gold output. Select a line to see its
median delay, p90 delay and share of stop departures at least three minutes late. The view
also shows the observation count and explains that many stop departures can belong to a
single bus journey. Missing measurements remain unknown.

The hosted viewer uses the complete **2026-10-29** Gold run: **3,515 lines with reports,
72,786 reported journeys and 1,618,184 timed stop departures**. The CSV includes all 3,992
line/day rows. It loads no landing data, starts no Spark job and needs no backend or
packages. Data comes from Entur under NLOD.

## Refresh from a Gold run

From the repository root, use the scorecard CSV and JSON run record produced by
`pipeline/gold_scorecard.py`:

```sh
uv run --no-project python examples/gold-scorecard/build.py \
  --csv /path/to/gold/line_scorecard/operating_date=2026-10-29/_preview/scorecard.csv \
  --run-record /path/to/gold/_runs/operating_date=2026-10-29/latest.json
```

The builder checks the source run IDs, date and totals, preserves null measurements, then
writes `dist/scorecard-data.js` and copies the complete CSV to `dist/scorecard.csv`. The
interface presents only lines with reported journeys or calls; the download contains all
line/day aggregates. `dist/index.html` works directly in a browser and on any static host.

Repeat `--csv` and `--run-record` in matching order to package multiple completed days.
Every day must be complete and ready in multi-day mode. The latest day is selected first;
the day selector switches between independent daily results. Daily percentiles are never
averaged into a weekly percentile. The combined CSV retains each row's operating date and
run IDs. Search covers all lines while the view renders at most 50 line choices and 25
comparison rows.

The hosted preview is a snapshot. To update it, rebuild from a new completed or explicitly
partial Gold run and republish the static bundle. The transit pipeline remains independent
of the frontend host.

Validation: the exported data reconciles to the [complete Gold run](../../docs/report/evidence/gold-2026-10-29.json).
Browser checks covered real counts/delays, search, missing measurements, national rendering
limits, long identifiers without a public code, and a 390 px mobile viewport, with no
JavaScript exceptions. Separate temporary fixtures checked day switching and independent
daily percentiles; none were published. See the [desktop screenshot](../../docs/report/evidence/gold-dashboard-2026-10-29.png).
The full-day update was published privately on 2026-11-05 from Site source commit
`85f9ec3db4e558a38c0f04b2afca83d139a41d66`.

The week continuation also rebuilds a local static bundle after each completed day at
`/data/dat535/silver-week-validation/frontend/`. Hosted publication is separate: the live
Site currently contains October 29 only. The [week evidence](../../docs/report/evidence/silver-week-2026-10-26-to-2026-11-01.md)
tracks the remaining dates.
