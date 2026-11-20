# Why ingest peaks in rush hour

The collector's ingest-rate panel peaks twice every weekday. This page says what the peak is made
of, with numbers from data already on the VM. No Entur requests were made for it.

Up: [evidence](README.md) · Related: [real-time feeds](../../entur/realtime.md),
[storage](../../system/storage.md#what-a-day-costs)

**Answer: mostly more service, with a smaller rise in update frequency.** At the 07:00 Oslo peak on
Thursday 2026-11-12, ET carried 2.14 times the bytes of 11:00. Planned journeys running rose 2.18
times. Restatements during planned trips per running journey rose 18 %, but fewer came outside
planned trips, so all restatements per running journey rose only 8 %. Each restatement was
9 % smaller. The 15:00 peak is the same: ×2.04 bytes, ×2.03 journeys. Message size and any single
codespace are ruled out. Most of that 18 % is Ruter's: 1.71 times as many restatements per
running journey at 07:00.

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

- ET is ~88 % of a weekday's uncompressed bytes (DATEX included) and makes the whole peak: 2.1 times
  midday. Vehicles rise 1.5 times. DATEX is flat at ~0.33 GB an hour. SX is ~0.4 MB in a median hour
  and at most 17 MB.
- Every weekday has the same shape. Weekends have none: traffic rises slowly to an afternoon high.
- Before the clock change on 2026-10-25 the peak came one UTC hour earlier (05 and 13 UTC instead of
  06 and 14). It follows Oslo local time, so it follows the timetable, not the collector.

## What makes the peak

[`rush_hour.py`](rush_hour.py) produced [`rush-hour-2026-11-12.json`](rush-hour-2026-11-12.json)
(Thursday) and [`rush-hour-2026-11-15.json`](rush-hour-2026-11-15.json) (Sunday) on 2026-11-20,
in 6 and 3 minutes on one core. Selected Oslo hours of the Thursday:

| Oslo hour | 03 | 07 | 08 | 11 | 15 | 16 | 21 |
| --------- | -: | -: | -: | -: | -: | -: | -: |
| ET GB | 0.12 | **14.40** | 13.75 | 6.72 | **13.68** | 12.94 | 4.11 |
| ET messages | 5,982 | 411,516 | 460,145 | 210,780 | 380,427 | 372,814 | 145,284 |
| Planned journeys running | 127 | **11,244** | 10,559 | 5,167 | **10,470** | 9,092 | 3,382 |
| Journey restatements | 8,676 | 843,832 | 823,520 | 358,054 | 760,088 | 707,244 | 226,795 |
| Restatements per running journey | 68 | 75 | 78 | 69 | 73 | 78 | 67 |
| Share during a planned trip | 47 % | 86 % | 90 % | 79 % | 88 % | 89 % | 90 % |
| In-trip restatements per running journey | 32 | 65 | 70 | 55 | 64 | 69 | 61 |
| KB per restatement | 13.6 | 16.8 | 16.3 | 18.4 | 17.7 | 18.0 | 17.7 |
| Calls per restatement | 25 | 31 | 31 | 34 | 32 | 33 | 34 |
| Vehicle reports | 66,507 | 735,185 | 721,438 | 491,585 | 710,186 | 719,012 | 339,004 |
| Vehicles reporting | 971 | 5,235 | 5,220 | 2,853 | 4,790 | 3,980 | 1,409 |
| Reports per vehicle | 68 | 140 | 138 | 172 | 148 | 181 | 241 |

Rush against 11:00, as factors of ET bytes = running journeys × restatements each × bytes each:

| Thursday | Bytes | Running journeys | Restatements each | Bytes each | In-trip restatements each |
| -------- | ----: | ---------------: | ----------------: | ---------: | ------------------------: |
| 07:00 | ×2.14 | ×2.18 | ×1.08 | ×0.91 | ×1.18 |
| 15:00 | ×2.04 | ×2.03 | ×1.05 | ×0.96 | ×1.16 |

- Running journeys double, and that is most of the peak. Counting only restatements during planned
  trips, there are 55 per running journey at 11:00 and 65 at 07:00. This is a normalized count,
  not a cadence: it does not weigh how many minutes of the hour each journey runs. More of the
  hour's restatements fall inside trips in rush (86 % against 79 %), so the total rises less.
- Restatements are a little smaller in rush: they hold ~31 calls against ~34 at 11:00.
- Messages double too, from 210,780 to 411,516, and carry 2.1 journeys each against 1.7.
  Only 1,446 of 4.9 million messages that day held no journey.
- Vehicles rise less than ET because each vehicle reports less often in rush. At 07:00, 1.83 times
  as many vehicles report, but reports rise only 1.50 times (×0.82 per vehicle).
- On Sunday 2026-11-15, 07:00 has 0.45 times the running journeys of 11:00 and 0.37 times the bytes.
  15:00 has 1.22 times the journeys and 1.30 times the bytes. In-trip restatements per running
  journey are ×0.93 and ×1.04. There is no peak because there is no rush.

By codespace, at 07:00 against 11:00 on the Thursday:

| Codespace | Share of 07:00 bytes | Bytes | Running journeys | In-trip restatements per running journey, 07:00 / 11:00 |
| --------- | -------------------: | ----: | ---------------: | ------------------------------------------------------: |
| ATB | 22 % | ×2.68 | ×2.87 | 132 / 126 (×1.05) |
| RUT | 19 % | ×1.73 | ×1.62 | **80 / 47 (×1.71)** |
| SKY | 13 % | ×2.68 | ×2.38 | 59 / 55 (×1.06) |
| KOL | 9 % | ×1.52 | ×1.83 | 78 / 76 (×1.03) |
| TRO | 8 % | ×2.68 | ×3.37 | 89 / 105 (×0.85) |

No single codespace makes the peak: the five largest carry 71 % of it, and each follows its own
running journeys. Ruter's update frequency adds to it: ×1.71 at 07:00 and ×1.49 at 15:00. Without
Ruter, in-trip restatements per running journey rise only ×1.05 and ×1.07. Ruter
sends no vehicle positions, so ET is its only real-time channel. TRO and VOT update less often in
rush (×0.85). Operators differ, from ~40 in-trip restatements per running journey (INN) to ~130 (ATB).

## Snapshots and resubscriptions

Both are full restatements that could look like a burst of updates. The collector journals show:

- ET and SX resubscribe once a day, at about 11:31 UTC, inside the 12:00 Oslo hour. This page
  therefore compares rush with 11:00.
- The vehicle websocket reconnected at 07:37, 09:22, 22:03 and 23:41 UTC on 11-12, and at 07:50 UTC
  on 11-15. A reconnect brings one snapshot of all vehicles, ~1,700 reports against 480,000 to
  760,000 an hour in Thursday daytime.

## Method

- Hours are UTC landing hours, and tables show the Oslo hour (UTC+1 in November).
- The panel's hourly value is the largest `.part` size scraped in the hour, every 30 s, counting
  JSON Lines. In hours above 100 MB, ET Bronze payload bytes are 0.2–9 % above it. Vehicles are
  9–11 % below it, since their JSON payloads are escaped inside the JSON Lines.
- ET is counted per `EstimatedVehicleJourney` element in every Bronze payload, since one push can
  hold several journeys from different codespaces. The decoded Silver cache can't be used for this:
  it drops journey states that didn't change. A journey's codespace is its `DataSource`.
  Journey elements hold 98 % of ET bytes. The rest is the message envelope.
- A planned journey is running in every hour its first planned departure to last planned arrival
  overlaps, from Silver `stop_call` of the three operating days around the date (`has_plan=true`).
  A restatement is matched to its plan on `DataFrameRef` and journey ID. Journey IDs recur daily,
  so a replay of another day's journey stays out of today's hours. VYG and BNR send no
  `DataFrameRef` but use dated IDs, which are matched on the ID alone.
- On the Thursday, 85 % of restatements fell in an hour their journey was running, 14 % in an hour
  outside its planned trip, and 0.2 % matched no plan, over half of them BNR. No BNR ID matches a plan.
- Vehicles come from the decoded Silver cache, which keeps reconnect duplicates.
  A vehicle reporting is a codespace and vehicle ID seen in the hour.
- Limits: two days, one weekday, and the weekday medians above show it is typical. Counting
  journeys that overlap an hour includes some that run only minutes of it. Ruter's higher update
  frequency is measured, not attributed to delays.
