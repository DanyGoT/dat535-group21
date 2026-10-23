-- Examples for: ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded
-- See README.md in this folder for column docs, joins, and gotchas.

-- All queries filter on operatingDate: the table is ~2.9B rows and partitioned
-- by operatingDate, so an unfiltered scan is very expensive.

-- 1. Recorded stops for one operating day.
SELECT
  operatingDate,
  dataSource,
  lineRef,
  datedServiceJourneyId,
  sequenceNr,
  stopPointName,
  aimedArrivalTime,
  arrivalTime,
  aimedDepartureTime,
  departureTime
FROM `ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded`
WHERE operatingDate = CURRENT_DATE() - 1
LIMIT 100;


-- 2. Arrival delay per stop (positive = late), for one day.
SELECT
  dataSource,
  lineRef,
  stopPointName,
  aimedArrivalTime,
  arrivalTime,
  TIMESTAMP_DIFF(arrivalTime, aimedArrivalTime, SECOND) AS arrival_delay_seconds
FROM `ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded`
WHERE operatingDate = CURRENT_DATE() - 1
  AND stopCancellation = FALSE
  AND arrivalTime IS NOT NULL
  AND aimedArrivalTime IS NOT NULL
ORDER BY arrival_delay_seconds DESC
LIMIT 100;


-- 3. Cancellation rates per operator for one day.
SELECT
  dataSource,
  dataSourceName,
  COUNT(*) AS stop_rows,
  COUNTIF(journeyCancellation) AS cancelled_journey_rows,
  COUNTIF(stopCancellation)    AS cancelled_stop_rows,
  ROUND(100 * COUNTIF(stopCancellation) / COUNT(*), 2) AS pct_stops_cancelled
FROM `ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded`
WHERE operatingDate = CURRENT_DATE() - 1
GROUP BY dataSource, dataSourceName
ORDER BY pct_stops_cancelled DESC;


-- 4. Reconstruct one vehicle journey's stops in order.
SELECT
  sequenceNr,
  stopPointName,
  aimedArrivalTime,
  arrivalTime,
  aimedDepartureTime,
  departureTime,
  stopCancellation
FROM `ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded`
WHERE operatingDate = CURRENT_DATE() - 1
  AND datedServiceJourneyId = 'replace-me'
ORDER BY sequenceNr;


-- 5. Mean arrival delay by line for one day (worst punctuality first).
SELECT
  dataSource,
  lineRef,
  COUNT(*) AS stops,
  ROUND(AVG(TIMESTAMP_DIFF(arrivalTime, aimedArrivalTime, SECOND)), 1) AS mean_arrival_delay_seconds
FROM `ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded`
WHERE operatingDate = CURRENT_DATE() - 1
  AND stopCancellation = FALSE
  AND arrivalTime IS NOT NULL
  AND aimedArrivalTime IS NOT NULL
GROUP BY dataSource, lineRef
HAVING stops >= 50
ORDER BY mean_arrival_delay_seconds DESC
LIMIT 50;


-- 6. Resolve the stop to an NSR Stop Place name (stopPointRef is a Quay id).
SELECT
  et.operatingDate,
  et.lineRef,
  et.stopPointName,
  sp.name AS stop_place_name,
  TIMESTAMP_DIFF(et.arrivalTime, et.aimedArrivalTime, SECOND) AS arrival_delay_seconds
FROM `ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded` et
JOIN `ent-data-sharing-ext-prd.national_stop_registry.quays_last_version` q
  ON q.id = et.stopPointRef
JOIN `ent-data-sharing-ext-prd.national_stop_registry.stop_places_last_version` sp
  ON sp.id = q.stopPlaceRef
WHERE et.operatingDate = CURRENT_DATE() - 1
LIMIT 100;
