# Entur real-time: SIRI and vehicle positions

What we collect from Entur in real time, how each channel behaves, and what the messages
contain. Sources: https://developer.entur.no/open-data/realtime.md, Entur's SIRI hub
`entur/anshar` (code read 2026-10-15), and our own data. Limits and licence: [entur/sources.md](sources.md).

## What we collect

| Feed | What it carries | Channel | Collector | Daily volume |
| ---- | --------------- | ------- | --------- | ------------ |
| **SIRI ET** (Estimated Timetable) | planned, expected and recorded times per call; cancellations; extra journeys | SIRI 2.0 pub/sub (push) | `receiver.py` | ~8.5 GB gzipped |
| **SIRI SX** (Situation Exchange) | disruption messages for lines and stops | SIRI 2.0 pub/sub (push) | `receiver.py` | ~2 MB |
| **Vehicle positions** | location, delay, occupancy, next stop per vehicle | GraphQL subscription (websocket) | `vehicles.py` | ~1.27 GB |

Not used: SIRI VM (vehicle positions as SIRI; the GraphQL API is richer) and GTFS-RT (no
national feed; partial per codespace, e.g. BNR only ET, SJN only SX).

## SIRI ET in one paragraph

ET holds the **current operating day only**. Each push restates whole journeys
(`EstimatedVehicleJourney`) that changed; as a vehicle passes a stop, its call moves from
`EstimatedCall` to `RecordedCall`. A call is restated on every change, about 57 times on
average ([pipeline/silver.md](../pipeline/silver.md)), so the raw stream is very redundant and Silver keeps the final state.

## Pub/sub: how the subscription works

Verified in `entur/anshar` (`Usage.md`, `Subscription-lifecycle.md`, the outbound routes and
`ServerSubscriptionManager`); the developer portal documents none of it. Values are the code's
defaults; production may override them (**UNVERIFIED**).

**Subscribe.** `POST https://api.entur.io/realtime/v1/subscribe` (or `/subscribe/<codespace>`)
with a SIRI 2.0 `SubscriptionRequest`. One subscription per data type, each with its own
`SubscriptionIdentifier`. Our request (built in `receiver.py`):

```xml
<Siri version="2.0" xmlns="http://www.siri.org.uk/siri">
  <SubscriptionRequest>
    <RequestTimestamp>…</RequestTimestamp>
    <Address>https://our-host/siri/et</Address>        <!-- required -->
    <RequestorRef>uis-dat535group21-collector</RequestorRef>
    <MessageIdentifier>uuid</MessageIdentifier>
    <SubscriptionContext><HeartbeatInterval>PT60S</HeartbeatInterval></SubscriptionContext>
    <EstimatedTimetableSubscriptionRequest>         <!-- or SituationExchangeSubscriptionRequest -->
      <SubscriberRef>…</SubscriberRef>
      <SubscriptionIdentifier>uis-dat535group21-collector-et</SubscriptionIdentifier>
      <InitialTerminationTime>…</InitialTerminationTime>   <!-- required, in the future -->
      <EstimatedTimetableRequest version="2.0"><RequestTimestamp>…</RequestTimestamp></EstimatedTimetableRequest>
      <ChangeBeforeUpdates>PT10S</ChangeBeforeUpdates>
    </EstimatedTimetableSubscriptionRequest>
  </SubscriptionRequest>
</Siri>
```

| Topic | Behaviour | What the receiver does |
| ----- | --------- | ---------------------- |
| Reply | `SubscriptionResponse` with `Status` true/false and an error text | logs and retries no sooner than 5 min |
| Duration | no maximum `InitialTerminationTime`; terminated once it passes | asks for 7 days, renews every 24 h |
| Initial data | right after accepting: everything current (ET: running, about to start, or changed; SX: all open messages), split into several POSTs (`MoreData`) | stores it like any push: **expect duplicates after every (re)subscribe** |
| Pushes | `HTTP POST`, `Content-Type: application/xml`, extra `subscriptionId` header; any 2xx accepted | appends, then answers 200 |
| Heartbeat | `HeartbeatNotification` to the same address, interval clamped to 30–60 s | stores it (coverage evidence); 3 missed = dead, re-subscribe |
| Failures | 15 s timeout per POST, one retry; after 5 failures in a row, backoff (10→60 s); cancelled after max(3 × heartbeat, 5 min) | answers fast; accepts `HEAD` |
| Re-accept | for 30 min after a cancellation, the same id is accepted only if a `HEAD` to our address connects | answers `HEAD` with 200 |
| Terminate | `TerminateSubscriptionRequest` (`RequestorRef` + `SubscriptionRef`) | sends it on shutdown |

Keep `useOriginalId` at its default (false): Entur then remaps ids to match the timetable, and
mixing settings would break joins ([entur/identifiers.md](identifiers.md)).

## Other SIRI channels (not used for collection)

- **Request/response:** `POST /services` (or `/services/<codespace>`). Reusing a `RequestorRef`
  returns only changes since the last call; `MoreData=true` means ask again.
- **SIRI Lite:** `GET /rest/et|sx|vm`, **4 requests/min**. Without `requestorId` the full
  dataset each time; with one (a UUID), changes since the last call (session ~5 min).
  `maxSize` defaults to 1500; `datasetId=<codespace>` filters. Since ET holds the whole
  current day, one full fetch can **recover a same-day receiver outage**; earlier days cannot
  be recovered. Response format (XML vs JSON): **UNVERIFIED**.

## Vehicle positions (GraphQL)

https://developer.entur.no/apis/graphql/vehicle-positions, verified 2026-10-17.

- Subscription: `wss://api.entur.io/realtime/v2/vehicles/subscriptions`, subprotocol
  `graphql-transport-ws` (the docs say "graphql-ws"). Queries:
  `POST https://api.entur.io/realtime/v2/vehicles/graphql`. `ET-Client-Name` required.
- `subscription { vehicles { ... } }` without a filter = all of Norway. The server batches
  updates (`bufferSize` 20, `bufferTime` 250 ms). Every new subscription starts with a
  snapshot of all current vehicles.
- **Ruter (RUT) publishes no positions**: `vehicles(codespaceId: "RUT")` is empty. Oslo is
  covered by ET only.
- Introspection works per type (`__type(name: ...)`); a full `__schema` dump is refused.
- Rate limits and history: not documented (**UNVERIFIED**).

First closed hour (2026-10-17 12:20–13:00 UTC, a Saturday): ~150 updates/s from 1,706
vehicles in 22 codespaces; 0.2 % exact repeats; received 5 s after `lastUpdated` at the median
(p99 24 s); 39 min = 369 MB raw, 39 MB gzipped. Field coverage: location, delay, line,
serviceJourney, occupancyStatus ~100 %; monitoredCall 92 %; progressBetweenStops 69 %;
datedServiceJourney 44 %; speed 10 %.

## What the messages contain

Verified on our data:

| Fact | Evidence |
| ---- | -------- |
| Modes in ET: bus 70 %, ferry 18 %, rail 11 %, tram 0.4 % of journey versions | 2026-10-22 08 UTC |
| Modes in vehicle positions: bus 93 %, ferry 4 %, coach 2 %, rail 1 %, tram 0.3 % of updates | same hour |
| Cancellations exist per journey (`EstimatedVehicleJourney/Cancellation`) and per call (`Cancellation`) | 59 journey-level `true` in 2026-10-22 06 UTC |
| Extra journeys: `ExtraJourney=true`, id in `EstimatedVehicleJourneyCode`, no `FramedVehicleJourneyRef` | 69 in the same hour |
| Some ET calls carry `Occupancy` (`seatsAvailable`, `standingAvailable`); which operators is not yet counted | 2026-10-22 08 UTC |
| Pushes reach 22.8M characters (Spark's JSON reader needs `maxStringLen`) | 2026-10-22, prod Bronze |

From Entur's BigQuery docs ([entur-ai-docs/realtime_siri_et/README.md](../../entur-ai-docs/realtime_siri_et/README.md)), same stream:

- Delay = actual − aimed. `RecordedAtTime` is when the message was published, not an arrival
  or departure time.
- **Some operators fill missing actual times with the aimed times**, which pulls delay
  averages toward zero. Check per operator before trusting punctuality numbers.
- Train data used to come only from Bane NOR (`BNR`); train companies later got their own
  streams.
- Per-operator data quality over time: https://data-quality.entur.no/SIRI_ET

Still **UNVERIFIED**: whether every operator sets the journey-level cancellation flag, and how
a stop passed without a `RecordedCall` shows up.
