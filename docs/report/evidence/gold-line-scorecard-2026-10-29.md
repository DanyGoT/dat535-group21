# Line reliability example — 2026-10-29

Complete Silver input.

Source Silver run: `local-20261105T005115Z-2f3d11a3`. Gold run: `local-20261105T051100Z-0345df18`.

3,992 lines; 76,222 planned journeys; 72,786 reported journeys; 1,618,184 scored departures.

Showing up to 25 lines with real-time reports, ordered by scored departures. The CSV and Parquet contain every line.

| Line | Operator | Planned journeys | Reported planned journeys | Unreported planned journeys | Scored departures | Late ≥3 min | Median delay (min) | P90 delay (min) | Reported cancelled journeys |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 31 Snarøya - Fornebu - Tonsenhagen - Grorud | Unibuss AS | 333 | 333 | 0 | 11846 | 27.5% | 1.7 | 5.8 | 0 |
| 79 Grorud T - Åsbråten | Connect Bus AS | 200 | 200 | 0 | 11178 | 47.2% | 2.7 | 11.6 | 0 |
| 37 Nydalen T - Helsfyr | Unibuss AS | 363 | 363 | 0 | 11039 | 32.0% | 1.8 | 6.5 | 0 |
| 2 Tjøme-Tønsberg-Horten-Holmestr | Tide avd. Vestfold | 126 | 126 | 0 | 10256 | 52.8% | 3.2 | 12.4 | 0 |
| 100 Kjeller - Oslo bussterminal | Nobina AS | 214 | 214 | 0 | 9376 | 49.8% | 3.0 | 14.7 | 0 |
| 81 Fornebu - Myrvoll stasjon | Connect Bus AS | 239 | 239 | 0 | 9006 | 44.7% | 2.5 | 9.4 | 0 |
| 5 5 | Connect Bus Norge | 139 | 139 | 0 | 8829 | 53.6% | 3.4 | 13.5 | 0 |
| 350 Lillestrøm - Ski | Vy Buss AS | 174 | 174 | 0 | 8557 | 37.1% | 2.3 | 6.1 | 0 |
| M1 Gulset-Skien-Langesund | Vy Buss avd. Grenland | 139 | 139 | 0 | 8198 | 42.4% | 2.5 | 7.9 | 0 |
| 6 6 | Connect Bus Norge | 131 | 130 | 1 | 8148 | 50.0% | 3.0 | 10.8 | 0 |
| 510 Bøleråsen - Ski - Ås - Drøbak | Connect Bus AS | 246 | 246 | 0 | 8145 | 44.4% | 2.5 | 10.3 | 0 |
| 4 Flaktveit - Hesjaholtet | Tide buss AS | 204 | 204 | 0 | 7972 | 43.4% | 2.3 | 8.7 | 0 |
| 25 Majorstuen-Haugerud | Unibuss AS | 210 | 210 | 0 | 7935 | 49.2% | 3.0 | 9.0 | 0 |
| 110 Lillestrøm - Oslo bussterminal | Nobina AS | 213 | 213 | 0 | 7850 | 46.8% | 2.7 | 11.5 | 0 |
| 20 Skøyen - Galgeberg | Unibuss AS | 338 | 338 | 0 | 7448 | 28.4% | 1.8 | 5.1 | 0 |
| 390 Kongskog/Hellerudhaugen - Oslo bussterminal | Connect Bus AS | 160 | 160 | 0 | 7334 | 32.0% | 1.9 | 5.8 | 0 |
| 250 Sætre - Slemmestad - Nationaltheatret | Vy Buss AS | 190 | 190 | 0 | 7255 | 38.4% | 2.3 | 7.9 | 0 |
| 1 Ranheim - Strindheim - sentrum - Tiller - Heimdal - Kattem | Vy | 249 | 249 | 0 | 6971 | 43.0% | 2.7 | 6.0 | 0 |
| 3 3 | Connect Bus Norge | 242 | 242 | 0 | 6969 | 36.1% | 2.1 | 6.8 | 3 |
| 23 Lysaker - Simensbråten | Nobina AS | 193 | 193 | 0 | 6928 | 27.3% | 1.4 | 8.3 | 0 |
| 4 4 | Connect Bus Norge | 163 | 163 | 0 | 6844 | 46.3% | 2.7 | 8.3 | 0 |
| 5 Sognsvann - Vestli | Sporveien T-banen AS | 168 | 168 | 0 | 6794 | 11.4% | 1.0 | 3.2 | 0 |
| 21 Tjuvholmen - Helsfyr T | Unibuss AS | 338 | 338 | 0 | 6759 | 34.9% | 2.2 | 6.4 | 0 |
| 1 Hessa - Myrland | Vy Buss | 173 | 173 | 0 | 6680 | 41.4% | 2.5 | 6.6 | 0 |
| 10 Sæterbakken- Strindheim- Sentrum- Ratesvingen | Vy | 180 | 180 | 0 | 6676 | 38.8% | 2.3 | 5.9 | 0 |

Interpretation:

- Late means actual departure minus SIRI aimed departure ≥180 seconds. Early departures are counted separately in the CSV.
- Delay statistics use non-cancelled reported calls with usable actual/aimed departures, boarding not forbidden, and no departure-before-arrival issue.
- Missing actuals are excluded from delay statistics; a zero denominator produces a blank rate, never 0%.
- Journey counts and stop-call counts are aggregated separately. Each journey is counted once regardless of its number of stops.
- Unreported journeys are unknown. Reported cancellations, timetable cancellations and timetable replacements are separate columns in the CSV; do not add them together.
- 10,884 scored departures have actual equal to aimed. This is source-quality evidence, not proof of punctuality.
- Labels are from the same operating day's Silver reference snapshots. No raw data is parsed by Gold.
