# Identifiers and joins

Sources: https://entur.atlassian.net/wiki/spaces/PUBLIC/pages/637370373/General+information+SIRI
and https://developer.entur.no/open-data/realtime.md. Inferences are marked.

## Join keys

| Real-time (SIRI ET) | Scheduled / reference | Notes |
| ------------------- | --------------------- | ----- |
| `DatedVehicleJourneyRef` | NeTEx `ServiceJourney` id | Must be equal (Entur requirement) |
| `FramedVehicleJourneyRef` → `DatedFrameRef` | operating date | ISO date, local time |
| stop refs (`StopPointRef`) | NSR Quay / StopPlace ids | NSR ids are mandatory in SIRI |
| `LineRef` | NeTEx `Line` id | Inference: same id scheme |

**Journey key = (ServiceJourney id, operating date).** A ServiceJourney runs on many days,
so the id alone is not unique.

## Gotchas

- **Operating day ≠ calendar day.** Late-night trips belong to the previous operating day.
  Never derive the operating date from a timestamp — use `DatedFrameRef`.
- **ID remapping:** Entur remaps real-time ids to match the static data by default.
  `useOriginalId=true` returns source-system ids that won't join. Keep the default.
- **Codespaces:** ids are prefixed by operator/authority codespace (e.g. `RUT`, `BNR`, `SJN`).
  The exact pattern `RUT:ServiceJourney:...` is **UNVERIFIED** — check real data.
- **GTFS vs NeTEx:** whether GTFS `trip_id` equals the NeTEx ServiceJourney id is
  **UNVERIFIED**. Use NeTEx for joins.
- **Schedule retention:** the static export keeps ≤48 h of past data. Join each operating day
  against the NeTEx archived on (or just before) that day, not the latest download.
- `DatedServiceJourney` exists in NeTEx; whether Entur's feeds use it is **UNVERIFIED**.
