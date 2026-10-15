# Entur real-time (SIRI)

Source: https://developer.entur.no/open-data/realtime.md
Base URL: `https://api.entur.io/realtime/v1`

## Feeds we use

- **SIRI ET (Estimated Timetable)** — primary. Covers delays, cancellations, additional
  departures and skipped stops. Holds the **current operating day only**. As a vehicle passes
  stops, calls move from `EstimatedCall` to `RecordedCall`.
  (https://entur.atlassian.net/wiki/spaces/PUBLIC/pages/637370373/General+information+SIRI)
- **SIRI SX (Situation Exchange)** — optional disruption/alert context.
- Not used: SIRI VM (vehicle positions); GTFS-RT, which has no national feed and partial
  coverage per codespace (e.g. BNR only ET, SJN only SX, VYX lacks ET/SX/trip updates).

## Access modes

### Publish/subscribe (SIRI 2.0 XML) — chosen for collection

- `POST /subscribe`: Entur pushes updates to an HTTP endpoint we host.
- Needs a reachable always-on receiver (see `../architecture.md`).

Verified 2026-10-15 against `entur/anshar` (Entur's SIRI hub that serves `/subscribe`):
`Usage.md`, `Subscription-lifecycle.md` and the outbound code (`routes/outbound/`,
`ServerSubscriptionManager`, `HeartbeatRoute`, `CamelRouteManager`; master at 2026-10-15).
The developer portal itself documents none of this. Config values below are the repo's
`application.properties` defaults; production may override them (**UNVERIFIED** for prod).

**Request.** `POST https://api.entur.io/realtime/v1/subscribe` (or `/subscribe/<codespace>`),
body a SIRI 2.0 `SubscriptionRequest`. One request per data type (ET and SX are separate
subscriptions, each with its own `SubscriptionIdentifier`; the address may be shared).
`?useOriginalId=true` is accepted here too — we keep the default. Shape (from
`Subscription-lifecycle.md`):

```xml
<Siri version="2.0" xmlns="http://www.siri.org.uk/siri">
  <SubscriptionRequest>
    <RequestTimestamp>…</RequestTimestamp>
    <Address>https://our-host/path</Address>           <!-- or ConsumerAddress; required -->
    <RequestorRef>…</RequestorRef>
    <MessageIdentifier>uuid</MessageIdentifier>
    <SubscriptionContext><HeartbeatInterval>PT60S</HeartbeatInterval></SubscriptionContext>
    <EstimatedTimetableSubscriptionRequest>            <!-- or SituationExchangeSubscriptionRequest -->
      <SubscriberRef>…</SubscriberRef>
      <SubscriptionIdentifier>our-unique-id</SubscriptionIdentifier>
      <InitialTerminationTime>…</InitialTerminationTime>   <!-- required, must be in the future -->
      <EstimatedTimetableRequest version="2.0">
        <RequestTimestamp>…</RequestTimestamp>
        <PreviewInterval>PT10H</PreviewInterval>
      </EstimatedTimetableRequest>
      <ChangeBeforeUpdates>PT10S</ChangeBeforeUpdates>
    </EstimatedTimetableSubscriptionRequest>
  </SubscriptionRequest>
</Siri>
```

- Synchronous reply: `SubscriptionResponse` with `ResponseStatus/Status` true/false and an
  error text (e.g. "ConsumerAddress must be set.", "InitialTerminationTime has already passed.").
- Reusing a `SubscriptionIdentifier` of a different type is rejected.

**Duration.** No maximum `InitialTerminationTime` in the code. The hub checks every 2 s and
terminates the subscription once that time passes (and posts a `TerminateSubscriptionResponse`
to our address). Renewal = send a new `SubscriptionRequest` before then.

**Delivery.** Right after accepting, Entur pushes an initial `ServiceDelivery` with all
current data (ET: journeys running, about to start, or with known changes; SX: all open
messages), then a `ServiceDelivery` per change. Large deliveries are split into several POSTs
(`MoreData`). Each push is `HTTP POST` to our address, `Content-Type: application/xml`, with an
extra `subscriptionId` header. We must answer 2xx; the body is discarded.

**Heartbeat.** `HeartbeatNotification` POSTed to the same address. Requested interval is
clamped to **30–60 s**. It is the *client's* job to notice silence and re-subscribe.

**Failures.** Each POST times out after 15 s (connect 5 s) and is retried once on transient
errors. Any non-2xx counts as a failure. After 5 consecutive 5xx/connection failures,
deliveries pause with backoff (10→60 s). If failures persist past the grace period
(max(3 × heartbeat, 5 min)), the subscription is **cancelled**. For 30 min afterwards, a new
`SubscriptionRequest` with the same id is only accepted if a `HEAD` probe to our address
connects (any HTTP status is fine).

**Termination.** `TerminateSubscriptionRequest` with `RequestorRef` + `SubscriptionRef`;
Entur replies with `TerminateSubscriptionResponse`. Re-subscribing resends all initial data.

Consequences for the receiver: answer 2xx fast (append, then reply), accept `HEAD`, treat
>3 missed heartbeats as a dead subscription and re-subscribe, renew well before
`InitialTerminationTime`, and expect duplicate data after every (re)subscribe.

### Request/response (SIRI 2.0 XML)

- `POST /services`, or per codespace `/services/<codespace>` (e.g. `/services/RUT`).
- Reusing the same `RequestorRef` returns only changes since the last call.
- `MoreData=true` → request again immediately for the next page.

### SIRI Lite (HTTP GET) — fallback / same-day recovery

- `/rest/et`, `/rest/sx`, `/rest/vm`. **4 requests/min.**
- Without `requestorId`: full dataset each call.
- With `requestorId` (UUID): the first call starts a session (~5 min timeout); later calls
  return changes only.
- `maxSize` defaults to 1500; `MoreData=true` means more is available.
- `datasetId=<codespace>` filters by codespace; `excludedDatasetIds` works for ET/VM only.
- Since ET holds the whole current operating day, one full fetch can recover a same-day
  receiver outage. Earlier days cannot be recovered.

## Parameters to keep fixed across all collection

- `useOriginalId`: default (false) remaps IDs to match the static data — **keep the default**,
  otherwise joins to NeTEx break. Never mix settings within the dataset.
- Response format of SIRI Lite (XML vs JSON): **UNVERIFIED**.

## Message content — to verify on real data

**UNVERIFIED** — check against Håndbok N801 / real payloads before designing the Silver schema:

- How cancellations are encoded (journey-level vs call-level flags).
- How extra/additional journeys are marked and identified.
- How missing recorded times appear (e.g. a stop passed without a `RecordedCall`).
