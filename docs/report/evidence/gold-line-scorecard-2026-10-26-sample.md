# Line reliability example — 2026-10-26

**PARTIAL SAMPLE — not a complete-day reliability result. Missing reports reflect this bounded input.**

Source Silver run: `local-20261028T144237Z-1a77a24e`. Gold run: `local-20261104T162507Z-4e6ebbc8`.

3,953 lines; 76,186 planned journeys; 8 reported journeys; 107 scored departures.

Showing up to 25 lines with real-time reports, ordered by scored departures. The CSV and Parquet contain every line.

| Line | Operator | Planned journeys | Reported planned journeys | Unreported planned journeys | Scored departures | Late ≥3 min | Median delay (min) | P90 delay (min) | Reported cancelled journeys |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 260E Bødalen - Slemmestad - Holmen - Sandvika- Ullevål | Vy Buss AS | 9 | 1 | 8 | 43 | 34.9% | 1.9 | 13.1 | 0 |
| 460 Ågotnes - Bergen | Tide buss AS | 94 | 1 | 93 | 31 | 35.5% | 1.4 | 14.2 | 0 |
| 20 Storavatnet-Haukeland sjukehus-Nesttun terminal | Tide Buss AS | 117 | 1 | 116 | 10 | 0.0% | 0.7 | 0.9 | 0 |
| 63 Grorud T - Romsås ring | Unibuss AS | 97 | 1 | 96 | 10 | 0.0% | 0.4 | 0.8 | 0 |
| 46 Majorstuen - Ullerntoppen | Vy Buss AS | 147 | 1 | 146 | 7 | 71.4% | 5.5 | 6.1 | 0 |
| 74 Jernbanetorget - Mortensrud T | Nobina AS | 171 | 1 | 170 | 4 | 0.0% | 2.2 | 3.0 | 0 |
| 44 Gravdal - Nipedalen | Tide Buss AS | 152 | 1 | 151 | 2 | 0.0% | 0.3 | 0.8 | 0 |
| 28 Åsane terminal - Ytre Arna - Arna terminal | Tide buss AS | 77 | 1 | 76 | 0 | — | — | — | 0 |

Interpretation:

- Late means actual departure minus SIRI aimed departure ≥180 seconds. Early departures are counted separately in the CSV.
- Delay statistics use non-cancelled reported calls with usable actual/aimed departures, boarding not forbidden, and no departure-before-arrival issue.
- Missing actuals are excluded from delay statistics; a zero denominator produces a blank rate, never 0%.
- Journey counts and stop-call counts are aggregated separately. Each journey is counted once regardless of its number of stops.
- Unreported journeys are unknown. Reported cancellations, timetable cancellations and timetable replacements are separate columns in the CSV; do not add them together.
- 0 scored departures have actual equal to aimed. This is source-quality evidence, not proof of punctuality.
- Labels are from the same operating day's Silver reference snapshots. No raw data is parsed by Gold.
