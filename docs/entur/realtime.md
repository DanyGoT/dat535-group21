# Entur real-time: SIRI and vehicle positions

This page explains collection protocols, payloads and coverage using our data and
`entur/anshar` code read on 2026-10-15.

Up: [Entur sources](sources.md) · Related: [identifiers](identifiers.md),
[Silver](../pipeline/silver.md)

## What we collect

October weekday landing volumes are approximate. See the dated [storage baseline](../system/storage.md#what-a-day-costs)
for weekday/weekend variation.

| Feed | Content | Channel / collector | Daily volume |
| ---- | ------- | ------------------- | -----------: |
| SIRI ET, Estimated Timetable | aimed, expected, recorded call times, cancellations, extra journeys | SIRI 2.0 push / [`receiver.py`](../../collector/receiver.py) | ~8.5 GB gzipped |
| SIRI SX, Situation Exchange | line and stop disruptions | SIRI 2.0 push / same receiver | ~2 MB |
| Vehicle positions | location, delay, occupancy, next stop | GraphQL websocket / [`vehicles.py`](../../collector/vehicles.py) | ~1.27 GB |

Documentation: https://developer.entur.no/open-data/realtime.md.

SIRI VM is unused because GraphQL is richer. GTFS-RT has no national feed and partial
codespace coverage, e.g. BNR only ET and SJN only SX.

## SIRI ET in one paragraph

ET serves the current operating day, but observed pushes also contain other-day records and
following-day replays. Each push restates changed `EstimatedVehicleJourney` elements.
Passed stops move from `EstimatedCall` to `RecordedCall`. In the exploratory 2026-10-22
sample, calls repeated about 57 times on average. See [the measurement](../pipeline/silver.md#exploratory-plan-matching-2026-10-22).
Silver selects final states and preserves prediction history.

## Pub/sub: how the subscription works

Provider defaults come from `entur/anshar`'s `Usage.md`, `Subscription-lifecycle.md`,
outbound routes and `ServerSubscriptionManager`. The portal omits them.
Production overrides remain **UNVERIFIED**.

[`receiver.py`](../../collector/receiver.py) posts a SIRI 2.0 `SubscriptionRequest` per
type to `https://api.entur.io/realtime/v1/subscribe`. A `/subscribe/<codespace>` suffix
can filter subscriptions. Each request needs its own `SubscriptionIdentifier`, callback
`Address` and future `InitialTerminationTime`. Ours use `<ET_CLIENT_NAME>-et` and
`<ET_CLIENT_NAME>-sx` as subscription ids. Our callbacks are `/siri/et` and `/siri/sx`.
It supplies timestamps, requestor/subscriber ids, a UUID, `HeartbeatInterval=PT60S`
and ET `ChangeBeforeUpdates=PT10S`. SIRI's namespace is `http://www.siri.org.uk/siri`.
An example callback address is `https://our-host/siri/et`.
The type-specific element is `EstimatedTimetableSubscriptionRequest` or
`SituationExchangeSubscriptionRequest`. Its inner request uses version `2.0` and a
`RequestTimestamp`.

| Topic | Provider behaviour | Our receiver |
| ----- | ------------------ | ------------ |
| Reply | `SubscriptionResponse` with `Status` and error text | logs failures, attempts at least 5 min apart |
| Duration | no maximum termination time, expires when reached | requests 7 days, renews after 24 h |
| Initial data | current ET journeys and all open SX messages, split with `MoreData` | stores all, including resubscription duplicates |
| Push | HTTP POST, `application/xml`, extra `subscriptionId` header, any 2xx accepted | appends before returning 200 |
| Heartbeat | `HeartbeatNotification`, interval clamped to 30–60 s | stores it as coverage evidence |
| Delivery failures | 15 s timeout, one retry, backoff 10→60 s after 5 consecutive failures | accepts pushes and HEAD probes |
| Cancellation | after max(3 × heartbeat, 5 min) of delivery failure | presumes dead after 3 heartbeat intervals without any push |
| Re-acceptance | same id within 30 min of cancellation requires a successful HEAD connection | returns 200 on callback HEAD |
| Termination | `TerminateSubscriptionRequest` with `RequestorRef` and `SubscriptionRef` | sends on shutdown |

Initial ET includes running, about-to-start or changed journeys.
Id remapping follows [identifiers](identifiers.md#ids).

## Other SIRI channels (not used for collection)

- Request/response uses `POST /services` or `/services/<codespace>`. Reuse `RequestorRef`
  for changes since the previous call. Ask again while `MoreData=true`.
- SIRI Lite uses `GET /rest/et|sx|vm`. Without `requestorId` it returns the full dataset.
  A UUID gives changes within a session of ~5 min.
- `maxSize` defaults to 1500. `datasetId=<codespace>` filters. Limits are in
  [sources](sources.md#usage-rules). XML versus JSON format remains **UNVERIFIED**.

A current-day fetch can recover ET's latest available state after an outage. It cannot
recover missed intermediate predictions or serve as an earlier-day raw archive.

## Vehicle positions (GraphQL)

https://developer.entur.no/apis/graphql/vehicle-positions, verified 2026-10-17.

| Item | Value |
| ---- | ----- |
| Subscription | `wss://api.entur.io/realtime/v2/vehicles/subscriptions` |
| Query | `POST https://api.entur.io/realtime/v2/vehicles/graphql` |
| Websocket subprotocol | `graphql-transport-ws`, despite the docs saying "graphql-ws" |
| Request header | `ET-Client-Name` |
| Unfiltered subscription | `subscription { vehicles { ... } }`, all Norway |
| Batching defaults | `bufferSize=20`, `bufferTime=250 ms` |

Every subscription starts with all current vehicles. `vehicles(codespaceId: "RUT")` is
empty because Ruter publishes no positions. Oslo is covered by ET only.
Type introspection with `__type(name: ...)` works, but full `__schema` is refused.
Rate limits and history remain **UNVERIFIED**.

First closed hour, 2026-10-17 12:20–13:00 UTC, a Saturday:

| Measurement | Result |
| ----------- | ------ |
| Throughput / coverage | ~150 updates/s, 1,706 vehicles, 22 codespaces |
| Exact repeats | 0.2 % |
| Receive latency from `lastUpdated` | median 5 s, p99 24 s |
| Size over 39 min | 369 MB raw, 39 MB gzipped |
| Location, delay, line, serviceJourney, occupancyStatus | ~100 % |
| monitoredCall / progressBetweenStops | 92 % / 69 % |
| datedServiceJourney / speed | 44 % / 10 % |

## What the messages contain

| Observation | Evidence |
| ----------- | -------- |
| ET modes, journey versions | bus 70 %, ferry 18 %, rail 11 %, tram 0.4 %, 2026-10-22 08 UTC |
| Vehicle modes, updates | bus 93 %, ferry 4 %, coach 2 %, rail 1 %, tram 0.3 %, same hour |
| Journey and call `Cancellation` exist | 59 journey-level `true`, 2026-10-22 06 UTC |
| `ExtraJourney=true` uses `EstimatedVehicleJourneyCode`, without `FramedVehicleJourneyRef` | 69 journeys, same hour |
| Some ET calls carry `Occupancy` | `seatsAvailable`, `standingAvailable`, 2026-10-22 08 UTC |
| Pushes reach 22.8M characters | 2026-10-22 prod Bronze, needs `maxStringLen` |

Occupancy coverage by operator has not been counted.

The [BigQuery catalog](../../entur-ai-docs/realtime_siri_et/README.md#gotchas) defines delay as actual
minus aimed. `RecordedAtTime` is publication time.
Some operators fill missing actuals with aimed times, pulling averages towards zero.
Check operator quality.

Train data originally came only from Bane NOR, `BNR`. Train companies later added streams.
Quality history: https://data-quality.entur.no/SIRI_ET.

Still **UNVERIFIED** are universal journey-cancellation coverage and how passing a stop
without a `RecordedCall` appears.
