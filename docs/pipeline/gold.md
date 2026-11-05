# Gold: use cases

Gold serves a named consumer; Silver stays general so several Gold products can share it
([pipeline/methodology.md](methodology.md)). This page lists the primary use case, independent ones that reuse the
same Silver, what each needs, and which DATEX feeds earn their keep. A manual
[daily line scorecard example](gold-example.md) implements a first slice of #2 over Entur
Silver. The prediction model, DATEX products and other use cases remain future work.

## At a glance

| # | Gold product | Consumer | Question | Needs beyond ET |
| - | ------------ | -------- | -------- | --------------- |
| 1 | **Departure risk** (primary) | traveller | "Will the 07:42 bus 3 from my stop be on time?" | NeTEx/NSR, DATEX situations + weather |
| 2 | Punctuality scorecard | public dashboard, transport authority | Which operators and lines run on time, and how often do they cancel? | NeTEx (planned denominator) |
| 3 | Delay hotspots | transit planners | Between which stops does a line lose time, and when? | NSR (coordinates) |
| 4 | Road incident impact | NPRA, municipalities | How many bus-minutes did this road work or accident cost? | DATEX situations, NSR or vehicle positions |
| 5 | Weather sensitivity | operators, winter planning | Which lines and modes break down on ice, snow or wind (ferries)? | DATEX weather + weather sites; Frost for ferries |
| 6 | Countdown accuracy | Entur, info displays | How far off is the predicted departure 5 / 10 / 20 min ahead? | none (all ET versions are in Bronze) |
| 7 | Disruption communication | customer information | Was this cancellation announced, and how early? | SX |
| 8 | Transfer reliability | journey planner | Will I make my connection at this hub? | NSR (stop places) |
| 9 | Crowding | traveller | Will there be a seat? | vehicle positions or ET occupancy |

For the course: **#1** as the main product, **#2** because it is cheap and lets us check our
Silver against Entur's own BigQuery numbers, and one DATEX product (**#5** or **#4**) to
show enrichment. The rest are options that the same Silver already supports.

Facts from real data that shape these (2026-10-22, hour 08 UTC): ET journey versions by mode
are bus 70 %, **ferry 18 %**, rail 11 %, tram 0.4 %; ET calls carry `Occupancy`; vehicle
positions cover bus, ferry, coach, rail and tram but **not Ruter (Oslo)**.

## 1. Departure risk (primary)

**Consumer and question.** A traveller looks up a departure (line, stop, planned time, date)
and gets: chance of leaving ≥ 3 min late, chance of cancellation, and the usual spread
("normally 0–2 min late; 1 in 5 is more than 5 min late at this hour"). With a reason when
there is one: active road works on the route, icy roads.

**Tables.**

- `departure_risk_profile`: one row per line × direction × quay × hour of week × day type
  (weekday, Saturday, Sunday/holiday): n, p(late ≥ 3 min), p(cancelled), delay p50/p90.
  Spark SQL aggregates over Silver stop calls. This alone answers the question for any
  future departure and is the **baseline** any model must beat.
- `departure_risk`: one row per planned departure (journey × stop × operating day) with
  the model's score and its top features. Spark MLlib (logistic regression, then gradient
  boosted trees) trained on Silver.

**Features** (all known *before* departure):

| Feature | From |
| ------- | ---- |
| line, operator, mode, direction, stop position in journey | Silver stop calls |
| hour of week, day type, school holiday | calendar (computed) |
| historical delay at this stop and hour (profile above) | Gold #1 profile |
| delay of the same journey at earlier stops, as known at lookup time (for "live" lookups) | Silver `call_prediction` (ET versions received by then), never final `stop_call` states |
| active road situations within X m of the route, by type | DATEX situations + NSR coordinates |
| road surface condition, temperature, precipitation, wind at nearest station | DATEX weather + weather sites |
| congestion index on matching road sections | DATEX travel times (while NPRA keeps them) |

**No leakage:** a feature may only use what was known at the lookup time. `stop_call` holds
final states, which include what happened after the lookup: they are labels, not features.

**Semantics to fix once** (Gold, not Silver): on time = less than 3 min late at departure
(arrival for the last stop); cancelled = journey or call cancelled; denominator = planned
calls (NeTEx), with "no real-time" as its own outcome, never counted as on time.

**Validation.** Split by time (train on earlier weeks, test on the next), never randomly:
neighbouring departures leak. Report calibration (do 30 % risks come true 30 % of the
time?), Brier score and AUC per mode. Two baselines: the profile table, and Entur's own
live estimate as it stood at the lookup time, a fixed time before the *planned* departure
(rebuilt from our ET versions). BigQuery `siri_et_estimated_times` counts its lead time back
from the *actual* departure, which nobody knows in advance; it fits #6, not this baseline.

**Limits.** Depth = weeks we have collected. Some operators copy aimed into actual times,
which hides delay ([entur/realtime.md](../entur/realtime.md)). For departures days ahead we only have current
conditions; a weather forecast feed (NPRA's road surface forecast, or MET) would be needed
for forecast features.

## 2. Punctuality scorecard

Per operator, line and mode, per day and week: share on time, share cancelled, mean and p90
delay, and **coverage** (planned journeys that ever appeared in real-time; 0–8 % never do,
[pipeline/silver.md](silver.md)). Grain: operator × line × mode × operating day. Small, easy to validate: for
calls that appear in ET, the same numbers can be computed from Entur's BigQuery
`realtime_siri_et_last_recorded` for the same day, which tests our Silver end to end.
BigQuery is ET only, so coverage and rates over planned journeys are checked against the
archived NeTEx instead.

## 3. Delay hotspots

Delay gained between consecutive stops (stop *n* to *n+1*) per line, direction and hour:
where lateness accumulates, and whether it is recovered at timing points. Grain: line ×
direction × stop pair × hour of week. With quay coordinates it becomes a map; with DATEX
travel times on the same roads it separates "road is slow" from "stop is slow" (dwell time).

## 4. Road incident impact

For each DATEX road situation (road works, closure, accident) and each transit leg within
X m of it during its validity: extra delay compared with the same leg, hour and day type
without an incident. Grain: situation × line × day. Answers "this road work cost line 3 an
extra 4 min per trip for six weeks". Records carry their own creation, version and validity
times, so 5-minute polling is enough. Needs a spatial join: situation location × quay
coordinates (NSR) or actual paths (vehicle positions).

## 5. Weather sensitivity

Delay and cancellation rates by road surface condition, temperature band, precipitation and
wind, per mode, region and line: which lines are fragile in winter, and for ferries, at what
wind speed cancellations start. Grain: line × weather bucket × month. Each quay maps to its
nearest road weather station (a small mapping table; the join must not change row counts).
Measured: DATEX road stations are a median 5.5 km from buses but 14 km from ferries and
inland, so the ferry half needs coastal stations from MET's Frost
([datex/content.md](../datex/content.md)).

## 6. Countdown accuracy

How wrong the real-time prediction is at fixed lead times (1, 5, 10, 20, 30 min before the
actual departure), per operator and line. Bronze keeps every ET version (~57 per call), so we
can rebuild what a passenger saw at any moment. Entur's BigQuery `siri_et_estimated_times`
does the same; comparing is a strong correctness check. Also the baseline for #1.

## 7. Disruption communication

Share of cancellations and large delays covered by an SX situation on that line or stop, and
how long before (or after) the disruption the message appeared. SX is tiny (~2 MB/day) and
already collected.

## 8. Transfer reliability

For planned transfers at a stop place (arrive on A, leave on B within a window), the chance
the connection holds given both delay distributions. Needs NSR to group quays into stop
places, and NeTEx for planned connections.

## 9. Crowding

Occupancy by line, direction and hour (`occupancyStatus` on ~100 % of vehicle positions,
`Occupancy` on some ET calls), and whether crowded trips run later (longer dwell times).

## What Silver must provide

One table per grain, shared by all of the above: `stop_call`, `journey`, `call_prediction`,
`vehicle_position`, `sx_situation`, the DATEX tables and reference tables. Catalog with
grains, keys and which Gold product uses which: [pipeline/silver.md](silver.md), section 2.

## DATEX: do we need all of it?

Yes, but the cost is almost all one feed. Measured per day ([datex/sources.md](../datex/sources.md), [system/storage.md](../system/storage.md)):

| Feed | Landing / day | Bronze / day | Used by | Verdict |
| ---- | ------------: | -----------: | ------- | ------- |
| situation | ~780 MB | ~520 MB | 1, 4 (core), 3 | keep: the main road signal for buses |
| weather + weather-sites | ~10 MB | ~13 MB | 1, 5 (core) | keep: nearly free, road surface is not in Frost |
| traveltime + traveltime-locations | ~3 MB | ~4 MB | 1, 3 | keep while NPRA serves it (end of life); build nothing that depends only on it |
| road surface forecasts | not collected | | 1 (future departures) | add only if #1 needs forecast features |
| webcams | not collected | | none | skip |

DATEX is ~7 % of what we store per day; SIRI ET is ~85 %.

## Order of work

1. ~~Archive NeTEx and NSR daily~~: done, first day 2026-10-26 ([system/architecture.md](../system/architecture.md)).
   #1, #2, #3, #4, #5 and #8 all need the plan or quay coordinates.
2. Silver `stop_call` and `journey`, checked against Entur's BigQuery for the same days.
3. Gold #2, then the #1 profile, then the #1 model against its baselines.
4. DATEX Silver (`road_situation`, `road_weather`, reference tables), then #5 or #4.
