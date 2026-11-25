# Silver quality report (2026-10-26 to 2026-11-23)

Generated from production GitHub Actions run-record artifacts (`prod-silver-runs-143`, workflow run
`45402302399`, 2026-11-24) using [`pipeline/summary.py`](../../../pipeline/summary.py):

```sh
python3 pipeline/summary.py quality <prod-silver-runs-dir>
```

Up: [evidence index](README.md) · Related: [Silver contracts](../../pipeline/silver.md),
[matched 2026-11-20 run records](README.md#scheduled-prod-runs-and-silver-quality-2026-11)

## Key findings

- **Before/after operating-date derivation & cached decoding:** `2026-10-26` (schema 1, before
  `normalize_journeys` date fallback and cached per-day decoding) quarantined **4,043,552** rows
  as `missing_operating_date` and took **3h 56m**. Across all 19 schema-2 days (`2026-10-29`,
  `2026-11-05`–`22`), quarantine dropped to **0 rows on 16 days and 9 rows total** (out of ~7.0 B
  decoded non-control versions), while weekday runtimes dropped to **37m–2h 23m** (typically
  ~1h 15m).
- **Live DATEX schema evolution caught automatically:** Starting on **2026-11-12**, Statens
  vegvesen began publishing a new XML quantity (`depositionDepth`) in `GetMeasuredWeatherData`.
  DATEX flagged facts jumped from ~800–1,050/day to **34,937** on Nov 12 and **~55,000/day**
  thereafter, surfaced cleanly as `unknown_weather_quantity_depositionDepth` (**634,073** rows
  across Nov 12–23) without failing publication or silently dropping the new element.
- **Plan coverage (denominator):** Weekday reporting coverage stayed steady at **96.9–97.6 %**
  (`2.4–3.1 %` planned calls unseen in real-time). Weekend coverage was **93.6–96.7 %**, with a
  notable `realtime_only` spike on Sunday `2026-11-22` (**69,814** calls vs ~14,000 on prior
  Sundays).
- **Operator `actual == aimed` evidence:** Over 20 days, `RUT:Operator:210` recorded **24.85 %**
  (`116,899 / 470,466`) exact aimed departures and `VYG:Operator:FLB` **31.16 %**, confirming why
  Silver tracks `actual_departure_equals_aimed` as source-quality evidence rather than assuming
  zero delay means perfect punctuality.
- **DATEX delivery resilience:** All 19 archived DATEX Silver days completed `ready=true` in
  **2m 03s–2m 46s** each, with 1 `source_delivery_failed` poll (`2026-11-17`) quarantined cleanly
  without blocking the day.

---

## Entur Silver funnel

| Operating day | Schema | Status | Bronze ET msgs (3d) | Decoded call ver | Retained call ver | Final `stop_call` | Superseded / dup removed | Quarantined | Elapsed |
| ------------- | -----: | ------ | ------------------: | ---------------: | ----------------: | ----------------: | -----------------------: | ----------: | ------: |
| 2026-10-26 | 1 | complete (ready) | 11,476,695 | 274,075,946 | 270,238,851 | 1,873,945 | 268,421,611 | 4,043,552 | 3h 56m |
| 2026-10-29 | 2 | complete (ready) | 14,322,202 | 421,717,344 | 139,758,004 | 1,873,467 | 137,936,960 | 0 | 2h 27m |
| 2026-11-05 | 2 | complete (ready) | 14,289,342 | 412,159,650 | 138,874,296 | 1,868,867 | 137,055,568 | 0 | 2h 12m |
| 2026-11-06 | 2 | complete (ready) | 12,379,337 | 348,046,381 | 135,638,555 | 1,845,599 | 133,846,738 | 0 | 37m 45s |
| 2026-11-07 | 2 | complete (ready) | 9,685,737 | 259,565,976 | 74,490,712 | 1,002,474 | 73,526,833 | 0 | 50m 28s |
| 2026-11-08 | 2 | complete (ready) | 9,757,180 | 258,630,289 | 49,368,415 | 704,327 | 48,700,790 | 0 | 49m 24s |
| 2026-11-09 | 2 | complete (ready) | 11,696,080 | 324,821,717 | 134,021,333 | 1,873,148 | 132,198,887 | 0 | 1h 10m |
| 2026-11-10 | 2 | complete (ready) | 14,284,200 | 412,426,124 | 140,282,299 | 1,866,862 | 138,462,306 | 0 | 1h 13m |
| 2026-11-11 | 2 | complete (ready) | 14,444,273 | 423,544,045 | 137,949,823 | 1,851,498 | 136,144,153 | 3 | 1h 42m |
| 2026-11-12 | 2 | complete (ready) | 14,469,086 | 425,736,005 | 145,214,024 | 1,869,675 | 143,397,504 | 3 | 1h 12m |
| 2026-11-13 | 2 | complete (ready) | 12,677,206 | 364,210,648 | 143,190,054 | 1,892,955 | 141,350,409 | 3 | 53m 27s |
| 2026-11-14 | 2 | complete (ready) | 9,917,302 | 269,180,576 | 76,625,168 | 1,000,245 | 75,657,911 | 0 | 36m 38s |
| 2026-11-15 | 2 | complete (ready) | 9,905,975 | 264,012,546 | 49,374,647 | 703,518 | 48,706,547 | 0 | 1h 20m |
| 2026-11-16 | 2 | complete (ready) | 11,802,733 | 325,356,239 | 137,236,692 | 1,851,591 | 135,433,563 | 0 | 1h 11m |
| 2026-11-17 | 2 | complete (ready) | 14,395,599 | 411,094,746 | 137,827,424 | 1,845,885 | 136,026,307 | 0 | 1h 24m |
| 2026-11-18 | 2 | complete (ready) | 14,452,012 | 411,451,287 | 135,929,193 | 1,868,135 | 134,106,033 | 0 | 1h 25m |
| 2026-11-19 | 2 | complete (ready) | 14,549,758 | 414,515,383 | 137,693,010 | 1,854,898 | 135,893,457 | 0 | 2h 23m |
| 2026-11-20 | 2 | complete (ready) | 12,813,862 | 355,735,770 | 141,542,829 | 1,879,676 | 139,721,874 | 0 | 1h 2m |
| 2026-11-21 | 2 | complete (ready) | 10,138,797 | 269,414,276 | 77,364,752 | 995,532 | 76,406,772 | 0 | 40m 51s |
| 2026-11-22 | 2 | complete (ready) | 9,986,253 | 266,823,161 | 50,531,057 | 760,168 | 49,814,767 | 0 | 58m 00s |

## Plan coverage (`stop_call`)

| Operating day | Planned calls | Matched | No real-time | Real-time only | Reporting share |
| ------------- | ------------: | ------: | -----------: | -------------: | --------------: |
| 2026-10-26 | 1,855,992 | 1,799,287 | 56,705 (3.06%) | 17,953 | 96.94% |
| 2026-10-29 | 1,855,391 | 1,802,968 | 52,423 (2.83%) | 18,076 | 97.17% |
| 2026-11-05 | 1,856,389 | 1,806,250 | 50,139 (2.70%) | 12,478 | 97.30% |
| 2026-11-06 | 1,834,491 | 1,780,709 | 53,782 (2.93%) | 11,108 | 97.07% |
| 2026-11-07 | 985,773 | 947,178 | 38,595 (3.92%) | 16,701 | 96.08% |
| 2026-11-08 | 690,306 | 653,604 | 36,702 (5.32%) | 14,021 | 94.68% |
| 2026-11-09 | 1,857,799 | 1,807,097 | 50,702 (2.73%) | 15,349 | 97.27% |
| 2026-11-10 | 1,854,991 | 1,808,122 | 46,869 (2.53%) | 11,871 | 97.47% |
| 2026-11-11 | 1,839,580 | 1,793,752 | 45,828 (2.49%) | 11,918 | 97.51% |
| 2026-11-12 | 1,856,100 | 1,802,945 | 53,155 (2.86%) | 13,575 | 97.14% |
| 2026-11-13 | 1,877,422 | 1,824,112 | 53,310 (2.84%) | 15,533 | 97.16% |
| 2026-11-14 | 987,058 | 954,070 | 32,988 (3.34%) | 13,187 | 96.66% |
| 2026-11-15 | 689,616 | 654,198 | 35,418 (5.14%) | 13,902 | 94.86% |
| 2026-11-16 | 1,837,396 | 1,788,934 | 48,462 (2.64%) | 14,195 | 97.36% |
| 2026-11-17 | 1,833,369 | 1,788,601 | 44,768 (2.44%) | 12,516 | 97.56% |
| 2026-11-18 | 1,857,032 | 1,812,057 | 44,975 (2.42%) | 11,103 | 97.58% |
| 2026-11-19 | 1,843,465 | 1,788,120 | 55,345 (3.00%) | 11,433 | 97.00% |
| 2026-11-20 | 1,868,840 | 1,810,119 | 58,721 (3.14%) | 10,836 | 96.86% |
| 2026-11-21 | 983,158 | 945,606 | 37,552 (3.82%) | 12,374 | 96.18% |
| 2026-11-22 | 690,354 | 646,476 | 43,878 (6.36%) | 69,814 | 93.64% |

## Entur quarantine (20 day(s))

Total quarantined: **4,043,561** of **7,316,817,311** decoded non-control versions (**0.06%**),
of which **4,043,552** came from the single schema-1 day (`2026-10-26`) and **9** across all 19
schema-2 days.

| Quarantine reason | Rows | Share of decoded versions |
| ----------------- | ---: | ------------------------: |
| `missing_operating_date` | 4,043,561 | 0.06% |

## Entur field-level `dq_issue` flags (20 day(s))

| Table | Rule | Flagged rows |
| ----- | ---- | -----------: |
| `vehicle_position` | `invalid_bearing` | 27,895,119 |
| `vehicle_position` | `unknown_quay` | 1,740,917 |
| `vehicle_position` | `missing_operating_date` | 1,689,808 |
| `vehicle_position` | `unknown_line` | 1,476,308 |
| `call_prediction` | `operating_date_derived` | 673,492 |
| `vehicle_position` | `invalid_progress` | 640,853 |
| `stop_call` | `unknown_quay` | 82,572 |
| `stop_call` | `unknown_line` | 59,476 |
| `stop_call` | `operating_date_derived` | 56,773 |
| `stop_call` | `missing_quay_mapping` | 30,594 |
| `stop_call` | `missing_quay` | 30,594 |
| `vehicle_position` | `conflicting_journey_id` | 18,800 |
| `journey` | `operating_date_derived` | 5,782 |
| `journey` | `unknown_line` | 3,681 |
| `stop_call` | `departure_before_arrival` | 3,520 |
| `stop_call` | `implausible_arrival_delay` | 869 |
| `vehicle_position` | `missing_journey_id` | 625 |
| `stop_call` | `implausible_departure_delay` | 624 |
| `sx_situation` | `invalid_validity_period` | 463 |
| `sx_situation` | `invalid_validity_start` | 420 |
| `stop_call` | `missing_aimed_time` | 1 |

## Operators with highest `actual_departure_equals_aimed` (min 100 actual departures)

| Operator | Actual departures | `actual == aimed` | Share |
| -------- | ----------------: | ----------------: | ----: |
| `VYG:Operator:FLB` | 215 | 67 | 31.16% |
| `RUT:Operator:210` | 470,466 | 116,899 | 24.85% |
| `VYG:Operator:TAG` | 156 | 10 | 6.41% |
| `MOR:Operator:240` | 573 | 33 | 5.76% |
| `SJN` | 15,112 | 576 | 3.81% |
| `SJN:Operator:SJN` | 8,500 | 286 | 3.36% |
| `GOA:Operator:GOA` | 35,716 | 703 | 1.97% |
| `MOR:Operator:254` | 18,365 | 302 | 1.64% |
| `VKT:Operator:75` | 1,117,672 | 15,794 | 1.41% |
| `NOR:Operator:69` | 459 | 5 | 1.09% |
| `NOR:Operator:298` | 188 | 2 | 1.06% |
| `SKY:Operator:NORLED` | 10,745 | 109 | 1.01% |
| `FLT:Operator:507` | 23,118 | 204 | 0.88% |
| `NOR:Operator:282` | 273 | 2 | 0.73% |
| `TRO:Operator:941` | 1,659 | 12 | 0.72% |

## DATEX Silver quality

| Observation day | Status | Situations | Weather rows | Travel-time rows | Flagged facts | Quarantined | Elapsed |
| --------------- | ------ | ---------: | -----------: | ---------------: | ------------: | ----------: | ------: |
| 2026-10-29 | complete (ready) | 3,430 | 63,663 | 58,176 | 980 | 0 | 2m 24s |
| 2026-11-05 | complete (ready) | 3,230 | 64,953 | 58,378 | 1,046 | 0 | 2m 30s |
| 2026-11-06 | complete (ready) | 3,156 | 66,078 | 58,176 | 822 | 0 | 2m 42s |
| 2026-11-08 | complete (ready) | 2,604 | 65,994 | 58,176 | 838 | 0 | 2m 33s |
| 2026-11-09 | complete (ready) | 3,098 | 65,966 | 58,176 | 768 | 0 | 2m 42s |
| 2026-11-10 | complete (ready) | 3,385 | 65,759 | 58,176 | 946 | 0 | 2m 22s |
| 2026-11-11 | complete (ready) | 3,167 | 66,031 | 58,176 | 941 | 0 | 2m 46s |
| 2026-11-12 | complete (ready) | 3,388 | 65,919 | 57,974 | 34,937 | 0 | 2m 16s |
| 2026-11-13 | complete (ready) | 3,172 | 65,803 | 58,176 | 54,956 | 0 | 2m 26s |
| 2026-11-14 | complete (ready) | 2,619 | 65,263 | 58,176 | 54,488 | 0 | 2m 10s |
| 2026-11-15 | complete (ready) | 2,777 | 65,628 | 57,974 | 54,712 | 0 | 2m 26s |
| 2026-11-16 | complete (ready) | 3,004 | 65,675 | 58,176 | 54,974 | 0 | 2m 22s |
| 2026-11-17 | complete (ready) | 3,085 | 64,340 | 57,166 | 53,960 | 1 | 2m 24s |
| 2026-11-18 | complete (ready) | 3,164 | 65,557 | 58,176 | 54,888 | 0 | 2m 22s |
| 2026-11-19 | complete (ready) | 2,994 | 65,732 | 58,176 | 55,278 | 0 | 2m 24s |
| 2026-11-20 | complete (ready) | 2,931 | 65,935 | 58,176 | 55,402 | 0 | 2m 05s |
| 2026-11-21 | complete (ready) | 2,385 | 66,161 | 58,176 | 55,512 | 0 | 2m 03s |
| 2026-11-22 | complete (ready) | 2,448 | 66,226 | 58,378 | 55,576 | 0 | 2m 41s |
| 2026-11-23 | complete (ready) | 2,914 | 66,176 | 58,378 | 55,570 | 0 | 2m 19s |

### DATEX `dq_issue` flags (19 day(s))

| Table | Rule | Flagged rows |
| ----- | ---- | -----------: |
| `road_weather` | `unknown_weather_quantity_depositionDepth` | 634,073 |
| `road_weather` | `suspect_precipitation` | 8,999 |
| `road_weather` | `suspect_humidity` | 7,507 |
| `road_weather` | `suspect_dew_point` | 2,048 |
| `road_weather` | `suspect_air_temperature` | 1,331 |
| `road_section` | `never_measured` | 1,029 |
| `road_weather` | `stale_measurement` | 436 |
| `weather_site` | `no_current_measurement` | 363 |
| `road_weather` | `suspect_wind` | 163 |

### DATEX quarantine reasons (19 day(s))

| Reason | Rows |
| ------ | ---: |
| `source_delivery_failed` | 1 |
