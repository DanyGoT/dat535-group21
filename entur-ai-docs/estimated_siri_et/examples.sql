-- Examples for: ent-data-sharing-ext-prd.realtime_siri_et.siri_et_estimated_times
-- See README.md in this folder for column docs, joins, and gotchas.

-- All queries filter on operatingDate: the table is ~9B rows and partitioned
-- by operatingDate, so an unfiltered scan is very expensive.

-- 1. Arrival estimate error 5 minutes before the stop, for one day.
--    Positive = the vehicle arrived later than estimated.
SELECT
  dataSource,
  lineRef,
  stopPointName,
  arrivalTime,
  estimatedArrivalTime,
  TIMESTAMP_DIFF(arrivalTime, estimatedArrivalTime, SECOND) AS arrival_error_seconds
FROM `ent-data-sharing-ext-prd.realtime_siri_et.siri_et_estimated_times`
WHERE operatingDate = CURRENT_DATE() - 1
  AND timeDeltaMinutes = 5
  AND arrivalTime IS NOT NULL
  AND estimatedArrivalTime IS NOT NULL
LIMIT 100;


-- 2. How accurate are estimates by lead time? Mean absolute arrival error
--    per timeDeltaMinutes for one day (expect error to grow with lead time).
SELECT
  timeDeltaMinutes,
  COUNT(*) AS calls,
  ROUND(AVG(ABS(TIMESTAMP_DIFF(arrivalTime, estimatedArrivalTime, SECOND))), 1) AS mean_abs_error_seconds
FROM `ent-data-sharing-ext-prd.realtime_siri_et.siri_et_estimated_times`
WHERE operatingDate = CURRENT_DATE() - 1
  AND arrivalTime IS NOT NULL
  AND estimatedArrivalTime IS NOT NULL
GROUP BY timeDeltaMinutes
ORDER BY timeDeltaMinutes;


-- 3. Estimate quality by operator at a fixed lead time (10 minutes out).
SELECT
  dataSource,
  transportMode,
  COUNT(*) AS calls,
  ROUND(AVG(ABS(TIMESTAMP_DIFF(arrivalTime, estimatedArrivalTime, SECOND))), 1) AS mean_abs_error_seconds
FROM `ent-data-sharing-ext-prd.realtime_siri_et.siri_et_estimated_times`
WHERE operatingDate = CURRENT_DATE() - 1
  AND timeDeltaMinutes = 10
  AND arrivalTime IS NOT NULL
  AND estimatedArrivalTime IS NOT NULL
GROUP BY dataSource, transportMode
ORDER BY mean_abs_error_seconds DESC;


-- 4. Estimate convergence for a single stop call: how the estimate changes
--    across lead times as the vehicle approaches.
SELECT
  timeDeltaMinutes,
  estimatedArrivalTime,
  arrivalTime,
  TIMESTAMP_DIFF(arrivalTime, estimatedArrivalTime, SECOND) AS arrival_error_seconds
FROM `ent-data-sharing-ext-prd.realtime_siri_et.siri_et_estimated_times`
WHERE operatingDate = CURRENT_DATE() - 1
  AND datedServiceJourneyId = 'ENT:DatedServiceJourney:replace-me'
  AND sequenceNr = 1
ORDER BY timeDeltaMinutes;


-- 5. Resolve the stop to an NSR Stop Place name (stopPointRef is a Quay id).
SELECT
  e.operatingDate,
  e.lineRef,
  e.timeDeltaMinutes,
  sp.name AS stop_place_name,
  e.stopPointName,
  TIMESTAMP_DIFF(e.arrivalTime, e.estimatedArrivalTime, SECOND) AS arrival_error_seconds
FROM `ent-data-sharing-ext-prd.realtime_siri_et.siri_et_estimated_times` e
JOIN `ent-data-sharing-ext-prd.national_stop_registry.quays_last_version` q
  ON q.id = e.stopPointRef
JOIN `ent-data-sharing-ext-prd.national_stop_registry.stop_places_last_version` sp
  ON sp.id = q.stopPlaceRef
WHERE e.operatingDate = CURRENT_DATE() - 1
  AND e.timeDeltaMinutes = 5
LIMIT 100;


-- 6. Lines with the least reliable far-ahead estimates (30 minutes out),
--    for one day. Useful for spotting where early estimates can't be trusted.
SELECT
  dataSource,
  lineRef,
  COUNT(*) AS calls,
  ROUND(AVG(ABS(TIMESTAMP_DIFF(arrivalTime, estimatedArrivalTime, SECOND))), 1) AS mean_abs_error_seconds
FROM `ent-data-sharing-ext-prd.realtime_siri_et.siri_et_estimated_times`
WHERE operatingDate = CURRENT_DATE() - 1
  AND timeDeltaMinutes = 30
  AND arrivalTime IS NOT NULL
  AND estimatedArrivalTime IS NOT NULL
GROUP BY dataSource, lineRef
HAVING calls >= 50
ORDER BY mean_abs_error_seconds DESC
LIMIT 50;
