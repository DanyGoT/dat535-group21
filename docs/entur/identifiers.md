# Identifiers and joins

How real-time records find their planned journey, stop and line. Sources:
https://entur.atlassian.net/wiki/spaces/PUBLIC/pages/637370373/General+information+SIRI,
https://developer.entur.no/open-data/realtime.md and our own data (dates given). Modelling
rules built on these keys: [pipeline/silver.md](../pipeline/silver.md).

## Keys

| Real-time (SIRI ET) | Plan / reference | Notes |
| ------------------- | ---------------- | ----- |
| `FramedVehicleJourneyRef/DataFrameRef` | operating date | local date; never derive it from a timestamp |
| `FramedVehicleJourneyRef/DatedVehicleJourneyRef` | NeTEx `ServiceJourney` id | equal by Entur's rules |
| call `Order` | `StopPointInJourneyPattern/@order` | position in the journey; the stop key (verified, [pipeline/silver.md](../pipeline/silver.md)) |
| `StopPointRef` | NSR **Quay**, via `ScheduledStopPoint` → `PassengerStopAssignment` | the Stop Place is the Quay's `stopPlaceRef` |
| `LineRef` | NeTEx `Line` id | same scheme (inferred) |

- **Journey key = (operating date, ServiceJourney id).** A ServiceJourney runs on many days.
- **Stop call key = journey key + `Order`.** Some journeys visit the same quay twice.
- Entur's BigQuery uses the same grain: `operatingDate + datedServiceJourneyId + sequenceNr`,
  with `datedServiceJourneyId = <operatingDate>:<serviceJourneyId>`.

## Ids

- Pattern `<codespace>:<Type>:<id>`, e.g. `SKY:ServiceJourney:30-208197-20940239` (in SIRI ET
  and NeTEx, 2026-10-22), `NSR:Quay:105273`. The codespace is the operator or authority
  (`RUT`, `SKY`, `VYG`, …).
- **Keep `useOriginalId` at its default.** Entur remaps real-time ids to the timetable's;
  `useOriginalId=true` returns source-system ids that won't join.
- **VYG trains** send `DatedVehicleJourneyRef = VYG:DatedServiceJourney:…` and no
  `FramedVehicleJourneyRef`. NeTEx maps a `DatedServiceJourney` to `ServiceJourneyRef` +
  `OperatingDayRef`, which gives the normal key. SKY has no `DatedServiceJourney` and uses
  `DayType` instead (2026-10-22/23).
- **Extra journeys** carry `EstimatedVehicleJourneyCode` and no `FramedVehicleJourneyRef`
  ([entur/realtime.md](realtime.md)); they have no planned counterpart.
- Silver resolves dated aliases from the selected archived plan. An ET record whose
  operating day still cannot be established goes to quarantine; neither its receive date
  nor a call's calendar date is silently used as the journey day. Vehicle positions retain
  a nullable operating day and are partitioned by UTC observation day instead.
- Direction vocabularies stay explicit: SIRI `direction_id`, NeTEx `direction_type`, vehicle
  API `direction_name`. Do not assume that numeric 1/2 maps consistently to inbound/outbound.
- GTFS `trip_id` = NeTEx ServiceJourney id? **UNVERIFIED**; join on NeTEx.

## Time

- **Operating day ≠ calendar day.** Night journeys belong to the day they started:
  `DataFrameRef` keeps the start day for calls after midnight. NeTEx times are local
  time-of-day plus `DayOffset`.
- **Join each day to the newest plan archived on or before that day** (`landing/netex/`), not
  the latest download: an export keeps ≤ 48 h of past days, often less, but always the coming
  days ([entur/sources.md](sources.md)).
- Our landing partitions are UTC ([system/architecture.md](../system/architecture.md)); an operating day spans two UTC dates.
- **Daylight saving.** Norway is UTC+2 in summer, UTC+1 in winter (switched 2026-10-25 03:00
  local). That night had 25 local hours: 02:00–03:00 happened twice. Convert local times with
  their offset or the `Europe/Oslo` zone, never a fixed +1/+2; expect a 23-hour operating day
  at the spring switch.
