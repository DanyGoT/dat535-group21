# Gold: product plan

This page defines proposed Gold products and their consumers. The manual
[Gold job](gold-example.md) delivers #2, a hub map for #3, an observed version of #8 and a
first slice of #4 and #5. Departure risk and the other products remain future work.

Up: [system overview](../report/overview.md#5-gold-answering-a-question) ·
Related: [Silver](silver.md#tables), [methodology](methodology.md#gold-serve-a-use-not-a-layer),
[Gold example](gold-example.md)

## At a glance

| # | Gold product | Consumer | Question | Needs beyond ET |
| - | ------------ | -------- | -------- | --------------- |
| 1 | **Departure risk** (primary) | Traveller | Will my departure be on time? | NeTEx/NSR, DATEX conditions |
| 2 | Punctuality scorecard | Authority, public dashboard | Which lines run on time or cancel? | NeTEx denominator |
| 3 | Delay hotspots | Transit planners | Where and when does a line lose time? | NSR coordinates |
| 4 | Road incident impact | NPRA, municipalities | How much extra delay accompanies an incident? | DATEX situations, transit locations |
| 5 | Weather sensitivity | Operators, winter planners | Which lines struggle with ice, snow or wind? | DATEX weather, Frost for ferries |
| 6 | Countdown accuracy | Entur, information displays | How wrong was the predicted departure? | Historical ET versions |
| 7 | Disruption communication | Customer information | Was a cancellation announced early enough? | SX |
| 8 | Transfer reliability | Journey planner | Will a connection hold? | NSR stop places, NeTEx |
| 9 | Crowding | Traveller | Will there be a seat? | Vehicle or ET occupancy |

Priorities are #1, #2 for validation, and #5 or #4 for enrichment. BigQuery comparison
remains planned. The other products need further matching and business rules.
[Observed feed coverage](../entur/realtime.md#what-the-messages-contain) shapes these choices.

## 1. Departure risk (primary)

**Consumer.** A traveller looks up a departure for late/cancellation risks, a usual delay
range and relevant road conditions.

**Output.** Two proposed tables:

- `departure_risk_profile` has one row per line × direction × quay × hour of week × day type.
  Day types are weekday, Saturday and Sunday/holiday. Spark SQL aggregates give sample size,
  late/cancellation shares and delay p50/p90. This is the baseline.
- `departure_risk` has one row per planned journey × stop × operating day, with a model
  score and top features. Start with MLlib logistic regression, then gradient-boosted trees.

**Inputs.** All features must be available before the departure lookup:

| Feature | Source |
| ------- | ------ |
| Line, operator, mode, direction, stop order | Silver stop calls |
| Hour of week, day type, school holiday | Computed calendar |
| Historical delay at this stop and hour | Profile fitted on earlier data |
| Earlier-stop delay known at lookup | `call_prediction`, never final `stop_call` |
| Nearby active road situations | DATEX situations and NSR coordinates |
| Surface condition, temperature, precipitation, wind | DATEX weather and station mapping |
| Congestion on matching sections | DATEX travel times while NPRA serves them |

Final `stop_call` timing states are labels, not live features. Proposed semantics count departure
delay ≥3 minutes as late, using arrival at the last stop. Cancellation means journey or
call cancellation. Use planned NeTEx calls as the denominator and keep missing real-time
reports as an unknown outcome.

**Validation and limits.** Train on earlier weeks and test on the next week. Neighbouring
departures can leak information under random splitting. Report calibration, Brier score and AUC
per mode. Compare with the profile and Entur's estimate at a fixed cutoff before the
*planned* departure.

BigQuery's `siri_et_estimated_times` measures lead time before *actual* departure, so it belongs
with #6 and cannot serve as this baseline.

An [ablation report](https://github.com/DanyGoT/dat535-group21/blob/experiments/silver-ablation/docs/report/silver-ablation.md)
tests Silver feature groups. It is a manual experiment on branch
`experiments/silver-ablation`, not a deployed prediction product.

History is limited to the weeks collected. Some operators copy aimed into actual times,
hiding delay. See [real-time source findings](../entur/realtime.md#what-the-messages-contain).
Days-ahead predictions would need forecasts from NPRA or MET rather than current conditions.

## 2. Punctuality scorecard

**Consumer.** An authority or dashboard reader compares operators, lines and modes.

**Output.** Proposed daily and weekly aggregates give on-time and cancellation shares,
mean/p90 delay and reporting coverage. Grain is operator × line × mode × operating day.
The [delivered scorecard](gold-example.md#line_scorecard) aggregates by line/day.

**Inputs.** Final calls and journeys, with archived NeTEx for the planned denominator.
The 2026-10-23 exploratory sample found 0–8 % of planned journeys absent from real-time.
See [Silver](silver.md#planned-journeys-unseen-in-et-2026-10-23) for scope and limits.

**Validation and limits.** Compare ET-covered services with BigQuery
`realtime_siri_et_last_recorded` for the same day. Check planned denominators against
NeTEx because BigQuery contains ET only. Neither missing reports nor complete input prove
punctuality. BigQuery access and contents remain
[unverified](../entur/sources.md#bigquery-history-reference).

## 3. Delay hotspots

**Consumer.** Transit planners locate where lateness accumulates or is recovered.

**Output.** Delay gained between consecutive stops, per line × direction × stop pair ×
hour of week.

**Inputs.** Stop calls and NSR coordinates for a map. DATEX travel times could help
separate slow roads from dwell time.

**Validation and limits.** Road matching needs explicit rules. Consecutive-call delay
alone does not identify the cause.

## 4. Road incident impact

**Consumer.** NPRA or municipalities assess road works, closures and accidents.

**Output.** Extra delay against the same leg, hour and day type without an incident.
Grain is situation × line × day.

**Inputs.** DATEX validity intervals and spatial matches to NSR quays or vehicle paths.
Record creation, version and validity times support five-minute polling, but events
shorter than the polling interval can be missed.

**Validation and limits.** Define distance and time windows, then reduce multiple matches
before joining transit facts. Proximity alone does not establish causation.

## 5. Weather sensitivity

**Consumer.** Operators plan for ice, snow and wind, including ferry cancellations.

**Output.** Delay and cancellation rates per line × weather bucket × month, compared
across modes and regions.

**Inputs.** DATEX weather and the quay-to-station mapping. An exploratory vehicle-to-station
check on 2026-10-26 at 08 UTC reported median distances of 5.5 km for buses and 14 km for
ferries. These describe vehicle positions, not the implemented quay mapping.
Inland stations mean ferries need coastal Frost observations.
See the [spatial sample](../datex/content.md#does-it-line-up-with-transit).

**Validation and limits.** Keep one weather match per quay before enrichment. Station
distance and source-quality flags constrain interpretation.

## 6. Countdown accuracy

**Consumer.** Entur or information-display maintainers assess countdown quality.

**Output.** Prediction error at 1, 5, 10, 20 and 30 minutes before *actual* departure,
per operator and line.

**Inputs.** Bronze ET versions, about 57 per call in the
[2026-10-22 sample](silver.md#exploratory-plan-matching-2026-10-22), or Silver's sampled
`call_prediction` states.

**Validation and limits.** Compare with BigQuery `siri_et_estimated_times`.
This retrospective cutoff uses the eventual actual departure. It is not the planned-time
lookup baseline for #1.

## 7. Disruption communication

**Consumer.** Customer-information teams assess whether passengers were warned.

**Output.** Share of cancellations and large delays covered by SX, and announcement lead
time.

**Inputs.** SX affected line/stop/journey IDs and validity times matched to final outcomes.

**Validation and limits.** Broad line alerts need matching rules and must not multiply calls.
Measure first observation across UTC partitions, as described in
[Silver](silver.md#entur).

## 8. Transfer reliability

**Consumer.** Journey planners estimate whether a passenger will make a connection.

**Output.** Connection success within a defined transfer window at a stop place.

**Inputs.** NeTEx planned connections, NSR quay groups and both journeys' delay distributions.

**Validation and limits.** Define the transfer window before estimating success.

**Delivered.** [`transfer_reliability`](gold-example.md#transfer_reliability) pairs each arrival
with the first departure of every other line aimed 0–15 min later at the same hub, and stores
the gap change per hour. NeTEx planned interchanges and walking times are not used yet; the
reader supplies spare minutes.

## 9. Crowding

**Consumer.** Travellers want to know whether seats are available.

**Output.** Occupancy by line × direction × hour, compared against delay.

**Inputs.** Vehicle `occupancyStatus` and ET `Occupancy`. The first vehicle sample had
occupancy on ~100 % of reports. ET coverage by operator is not yet counted. See
[vehicle coverage](../entur/realtime.md#vehicle-positions-graphql).

**Validation and limits.** Coverage differs by operator. Ruter publishes no vehicle
positions in the observed feed, so this cannot represent all services.

## What Silver must provide

[Silver](silver.md#tables) owns the implemented grains, keys and partitions for `stop_call`,
`journey`, `call_prediction`, vehicle positions, SX, DATEX facts and reference mappings.
Gold still defines thresholds, denominators and joins between sources.

The DATEX feeds are collected and have Silver tables. Keep situations for #1/#4, weather
for #1/#5 and travel times for #1/#3 while NPRA serves them. Build no product that depends
only on the end-of-life travel-time feed. Add forecasts only if #1 needs them and skip
webcams. Costs are in [DATEX sources](../datex/sources.md#cost-per-day) and
[storage](../system/storage.md#what-a-day-costs).

## Order of work

1. Daily reference archives and Entur/DATEX Silver are implemented. BigQuery checks remain planned.
2. Done: Gold reads schema-2 Silver and DATEX, and publishes #2, the #3 map, #8 and a condition
   comparison for #4/#5 ([what is built](gold-example.md)).
3. Build the #1 profile, then the model and its planned-time baselines.
4. Turn the condition comparison into per-line #4/#5 products, with Frost for ferries.
