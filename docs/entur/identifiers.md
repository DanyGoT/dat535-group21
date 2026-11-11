# Identifiers and joins

This page explains how real-time records map to planned journeys, stops and lines, and defines
source vocabulary and time conventions.

Up: [Entur sources](sources.md) · Related: [real-time feeds](realtime.md),
[Silver contracts](../pipeline/silver.md)

## Keys

| SIRI ET field | Plan / reference | Meaning |
| ------------- | ---------------- | ------- |
| `FramedVehicleJourneyRef/DataFrameRef` | operating date | reported local journey day |
| `FramedVehicleJourneyRef/DatedVehicleJourneyRef` | NeTEx `ServiceJourney` id | equal under Entur's mapping |
| Call `Order` | `StopPointInJourneyPattern/@order` | journey position, verified in [Silver](../pipeline/silver.md#exploratory-plan-matching-2026-10-22) |
| `StopPointRef` | NSR Quay | plan maps through `ScheduledStopPoint` → `PassengerStopAssignment` |
| `LineRef` | NeTEx `Line` id | same scheme, inferred |

Journey key is `(operating date, ServiceJourney id)`. Add `Order` for a stop-call key,
because a journey can revisit a quay. The quay's `stopPlaceRef` identifies its Stop Place.

BigQuery uses `operatingDate + datedServiceJourneyId + sequenceNr`, with
`datedServiceJourneyId = <operatingDate>:<serviceJourneyId>`.

## Ids

Ids follow `<codespace>:<Type>:<id>`, e.g. `SKY:ServiceJourney:30-208197-20940239`
in ET and NeTEx on 2026-10-22, or `NSR:Quay:105273`. Codespaces identify operators or
authorities, e.g. `RUT`, `SKY`, `VYG`.

Keep `useOriginalId=false`. Entur remaps source-system ids to timetable ids for joins.

VYG trains report `DatedVehicleJourneyRef=VYG:DatedServiceJourney:…` without
`FramedVehicleJourneyRef`. NeTEx resolves this through `ServiceJourneyRef` and
`OperatingDayRef`. SKY uses `DayType` without `DatedServiceJourney`, checked
2026-10-22/23. Extra journeys use `EstimatedVehicleJourneyCode` without a framed reference
or planned counterpart ([real-time feeds](realtime.md#what-the-messages-contain)).

[`normalize_journeys`](../../pipeline/silver.py) looks up explicit dated references before
ordinary journey ids. ET may carry the alias in the journey id itself. It retains reported
dates, flags mapping conflicts and preserves source ids.

Operating-date precedence is reported date, archived-plan alias, date embedded in the id,
then the first call's aimed departure or arrival converted to an Oslo date. The last two set `operating_date_derived` and
`date_source`. The last fallback can misdate night journeys. Unresolved ET identities go to
quarantine. Receive date is never the fallback. Vehicles retain nullable operating dates and
use UTC observation partitions.

Keep direction vocabularies separate. SIRI has `direction_id`, NeTEx `direction_type`,
and vehicles `direction_name`. Numeric 1/2 need not mean inbound/outbound consistently.
GTFS `trip_id` equality with NeTEx ServiceJourney remains **UNVERIFIED**. Join on NeTEx.

## Time

- Operating day differs from calendar day. After-midnight calls keep the journey's starting
  day in `DataFrameRef`. NeTEx uses local time-of-day plus `DayOffset`.
- Select the newest archive on or before the operating day, never today's latest download.
  Export history is limited ([sources](sources.md#timetable-netex-gtfs)).
- Landing dates are UTC ([architecture](../system/architecture.md#landing-zone)). Operating days cross
  UTC boundaries.
- Norway uses UTC+2 in summer and UTC+1 in winter. On 2026-10-25 at 03:00 local, clocks went
  back and 02:00–03:00 repeated, giving 25 local hours. Spring gives 23 hours.
  Convert using the supplied offset or `Europe/Oslo`, never a fixed +1/+2.

Source references:
https://entur.atlassian.net/wiki/spaces/PUBLIC/pages/637370373/General+information+SIRI
and https://developer.entur.no/open-data/realtime.md.
