# Why ingest peaks in rush hour

The collector's ingest-rate panel peaks twice every weekday. This page says what the peak is made
of, with numbers from data already on the VM. No Entur requests were made for it.

Up: [evidence](README.md) · Related: [real-time feeds](../../entur/realtime.md),
[storage](../../system/storage.md#what-a-day-costs)

**Answer: more service.** At the 07:00 Oslo peak on Thursday 2026-11-12, ET carried 1.97 times
the bytes of 12:00. Twice as many planned journeys were running (×2.07), each was restated about as
often as at midday (×1.04), and each restatement was slightly smaller (×0.91). The 15:00 peak is
the same: ×1.87 = ×1.92 × ×1.01 × ×0.96. Updates per journey and message size do not make the
peak. Ruter is the one exception: its journeys are pushed a third more often in the morning rush.

## The peak, every weekday since 2026-10-16

The panel plots uncompressed bytes landed per hour (`dat535_landing_open_bytes`). Hourly values from
Prometheus are in [`rush-hour-panel-2026-10-16-to-11-19.json`](rush-hour-panel-2026-10-16-to-11-19.json).
Medians over 2026-10-26 to 11-19, by Oslo hour (UTC+1), in GB:

| Oslo hour | 03 | 05 | 06 | 07 | 08 | 11 | 12 | 15 | 16 | 18 | 21 | Day |
| --------- | -: | -: | -: | -: | -: | -: | -: | -: | -: | -: | -: | --: |
| ET, weekday | 0.12 | 2.05 | 7.82 | **13.90** | 12.03 | 6.68 | 7.53 | **13.55** | 12.82 | 6.77 | 4.06 | 151 |
| ET, Saturday | 0.56 | 0.38 | 1.40 | 2.46 | 3.27 | 5.36 | 5.74 | 6.02 | 5.90 | 4.91 | 3.36 | 83 |
| ET, Sunday | 0.61 | 0.27 | 0.68 | 1.04 | 1.30 | 2.87 | 3.46 | 3.70 | 4.03 | 4.09 | 3.28 | 57 |
| Vehicles, weekday | 0.07 | 0.27 | 0.69 | **0.83** | 0.82 | 0.56 | 0.60 | **0.80** | 0.80 | 0.56 | 0.41 | 12 |

- ET is ~88 % of a weekday's uncompressed bytes (DATEX included) and makes the whole peak: 2.1 times midday.
  Vehicles rise 1.5 times, SX stays under 3 MB an hour, DATEX is flat at ~0.33 GB an hour.
- Every weekday has the same shape. Weekends have none: traffic rises slowly to an afternoon high.
- Before the clock change on 2026-10-25 the peak came one UTC hour earlier (05 and 13 UTC instead of
  06 and 14). It follows Oslo local time, so it follows the timetable, not the collector.

## What makes the peak

[`rush_hour.py`](rush_hour.py) produced [`rush-hour-2026-11-12.json`](rush-hour-2026-11-12.json)
(Thursday) and [`rush-hour-2026-11-15.json`](rush-hour-2026-11-15.json) (Sunday) on 2026-11-20,
in 6 and 3 minutes on one core. Selected Oslo hours of the Thursday:

| Oslo hour | 03 | 07 | 08 | 12 | 15 | 16 | 21 |
| --------- | -: | -: | -: | -: | -: | -: | -: |
| ET GB | 0.12 | **14.40** | 13.75 | 7.30 | **13.68** | 12.94 | 4.11 |
| ET messages | 5,982 | 411,516 | 460,145 | 206,016 | 380,427 | 372,814 | 145,284 |
| Planned journeys running | 127 | **11,244** | 10,559 | 5,441 | **10,470** | 9,092 | 3,382 |
| Journey restatements | 8,676 | 843,832 | 823,520 | 391,181 | 760,088 | 707,244 | 226,795 |
| Restatements per running journey | 68 | 75 | 78 | 72 | 73 | 78 | 67 |
| KB per restatement | 13.6 | 16.8 | 16.3 | 18.3 | 17.7 | 18.0 | 17.7 |
| Calls per restatement | 25 | 31 | 31 | 33 | 32 | 33 | 34 |
| Vehicle reports | 66,507 | 735,185 | 721,438 | 521,335 | 710,186 | 719,012 | 339,004 |
| Vehicles reporting | 971 | 5,235 | 5,220 | 3,118 | 4,790 | 3,980 | 1,409 |
| Reports per vehicle | 68 | 140 | 138 | 167 | 148 | 181 | 241 |

Rush against 12:00, as factors of ET bytes = running journeys × restatements each × bytes each:

| Thursday | Bytes | Running journeys | Restatements each | Bytes each |
| -------- | ----: | ---------------: | ----------------: | ---------: |
| 07:00 | ×1.97 | ×2.07 | ×1.04 | ×0.91 |
| 15:00 | ×1.87 | ×1.92 | ×1.01 | ×0.96 |

- A running journey is restated about 70 times an hour, roughly every 50 s, at any time of day.
  Rush adds journeys, not updates per journey.
- Restatements are a little smaller in rush: they hold ~31 calls against ~33 at midday.
- Messages double too, from 206,016 to 411,516, at about two journeys each. Only 1,446 messages of
  4.9 million were heartbeats or other messages without a journey.
- Vehicles rise less than ET because each vehicle reports less often in rush (×0.84 at 07:00):
  1.68 times as many vehicles report, but reports rise only 1.41 times.
- On Sunday 2026-11-15, 07:00 has 0.42 times the running journeys of 12:00 and 0.31 times the bytes.
  15:00 has 1.14 times the journeys and 1.07 times the bytes. There is no peak because there is no rush.

By codespace, at 07:00 against 12:00 on the Thursday:

| Codespace | Share of 07:00 bytes | Bytes | Running journeys | Restatements each, 07:00 / 12:00 |
| --------- | -------------------: | ----: | ---------------: | -------------------------------: |
| ATB | 22 % | ×2.44 | ×2.52 | 156 / 154 |
| RUT | 19 % | ×1.84 | ×1.59 | **83 / 63** |
| SKY | 13 % | ×2.11 | ×2.24 | 71 / 72 |
| KOL | 9 % | ×1.49 | ×1.78 | 105 / 111 |
| TRO | 8 % | ×2.34 | ×3.19 | 102 / 129 |

No single codespace makes the peak: the five largest carry 71 % of it, and each scales with its own
running journeys. Ruter (RUT) is the exception where update frequency adds to it: ×1.32 per journey
at 07:00 and ×1.18 at 15:00. Ruter sends no vehicle positions, so ET is its only real-time channel.
Restatement rates differ by operator, from ~40 an hour (INN) to ~155 (ATB).

## Snapshots and resubscriptions

Both are full restatements that could look like a burst of updates. The collector journals show:

- ET and SX resubscribe once a day, at about 11:31 UTC (12:31 Oslo), outside both peaks.
- The vehicle websocket reconnected at 07:37, 09:22, 22:03 and 23:41 UTC on 11-12, and at 07:50 UTC
  on 11-15. A reconnect brings one snapshot of all vehicles, ~1,700 reports against 480,000 to
  760,000 an hour in Thursday daytime.

## Method

- Hours are UTC landing hours, and tables show the Oslo hour (UTC+1 in November).
  Bronze payload bytes per hour match the panel's series within 2 %, except 7 % at 23:00.
- ET is counted per `EstimatedVehicleJourney` element in every Bronze payload, since one push can
  hold several journeys from different codespaces. The decoded Silver cache can't be used for this:
  it drops journey states that didn't change. A journey's codespace is its `DataSource`.
  Journey elements hold 98 % of ET bytes. The rest is the message envelope.
- A planned journey is running in every hour its first planned departure to last planned arrival
  overlaps, from Silver `stop_call` of the three operating days around the date (`has_plan=true`).
  86 % of restatements fall in an hour their journey is running, 14 % in an hour outside its planned
  trip, and 0.3 % match no planned journey.
- Vehicles come from the decoded Silver cache, which keeps reconnect duplicates.
  A vehicle reporting is a codespace and vehicle ID seen in the hour.
- Limits: two days, one weekday, and the weekday medians above show it is typical. Ruter's higher
  update frequency is measured, not attributed to delays.
