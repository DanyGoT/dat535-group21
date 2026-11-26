#!/usr/bin/env python3
"""Daily Gold over published Silver: line scorecard, stop map, transfer chances, line delays,
delay between stops and road conditions.

Reads one Norwegian operating day of Entur Silver and the same UTC day of DATEX Silver.
Every count is additive: consumers sum days, never average rates. Partial Silver needs
--allow-partial and gives a non-ready result. Details: docs/pipeline/gold-example.md.
"""

import argparse
import csv
import json
import os
import shutil
import sys
import time
import uuid
from datetime import date as Date, datetime, timedelta, timezone
from pathlib import Path

from bronze import create_spark, iso, run_id, utcnow
from silver import assert_unique, atomic_json, functions, publish

# Per source: (marker folder under silver/, partition column, marker schema version, tables read).
SOURCES = {
    "entur": (
        Path("_runs"),
        "operating_date",
        2,
        ("stop_call", "journey", "line", "operator", "quay", "stop_place"),
    ),
    "datex": (
        Path("_runs") / "datex",
        "observation_date",
        1,
        ("road_weather", "weather_site", "road_situation_interval", "road_travel_time",
         "quay_weather_site", "quay_road_situation", "quay_road_section"),
    ),
}

# The key of each Gold table.
KEYS = {
    "line_scorecard": ["operating_date", "line_id"],
    "stop_reliability": ["operating_date", "hub_id", "hour"],
    "transfer_reliability": [
        "operating_date", "hub_id", "from_line_id", "from_origin_id",
        "to_line_id", "to_destination_id", "hour",
    ],
    "line_stop_delays": ["operating_date", "hub_id", "line_id", "direction", "end_id", "hour"],
    "segment_delay": ["operating_date", "line_id", "from_hub_id", "to_hub_id", "hour"],
    "departure_conditions": [
        "operating_date", "mode", "hour", "precipitation", "road_surface",
        "road_situation", "traffic",
    ],
}

UNKNOWN_LINE, UNKNOWN_HUB = "__unknown_line__", "__unknown_hub__"
LATE_S = 180

# Departure delay bins in seconds, [low, high). "Late" (>= 3 min) is the last three summed.
DELAY_BINS = (
    ("early", None, -60),
    ("on_time", -60, 60),
    ("1_3_min", 60, 180),
    ("3_5_min", 180, 300),
    ("5_10_min", 300, 600),
    ("10_min_plus", 600, None),
)

# Transfers: departures up to 15 min after the arrival count as connections.
MAX_SLACK_S = 15 * 60
# Gap change in whole minutes, clipped to this range. A connection planned with b spare
# minutes holds when the change is >= -b (exact for b in 0..10).
GAP_LOW, GAP_HIGH = -11, 10
# Arrival and departure delay per line at a hub in whole minutes, floor(delay / 60), clipped.
DELAY_LOW, DELAY_HIGH = -10, 30
# A segment's delay change is clipped to +-15 min, so one stranded vehicle cannot carry a mean.
MAX_CHANGE_S = 15 * 60
WEATHER_MAX_M = 10_000

# DATEX record types for something going wrong on the road (not planned roadworks).
INCIDENTS = (
    "Accident", "AnimalPresenceObstruction", "EnvironmentalObstruction", "GeneralObstruction",
    "InfrastructureDamageObstruction", "VehicleObstruction", "RoadsideAssistance",
    "PoorEnvironmentConditions", "WeatherRelatedRoadConditions",
    "NonWeatherRelatedRoadConditions", "EquipmentOrSystemFault",
)


# --- Reading Silver ---


def source_record(output, source, day, allow_partial=False, optional=False):
    """The Silver marker for one source and day. Refused unless that day is published."""
    runs, partition, schema, _ = SOURCES[source]
    path = Path(output) / "silver" / runs / f"{partition}={day}" / "latest.json"
    if optional and not path.exists():
        return None
    record = json.loads(path.read_text())
    if record.get("schema_version") != schema or record.get(partition) != str(day):
        raise ValueError(f"{source} Silver marker has an unsupported schema or wrong date")
    if record.get("status") not in {"complete", "partial"}:
        raise ValueError(
            f"{source} Silver is not published: refusing a failed or publishing marker"
        )
    ready = record.get("status") == "complete" and record.get("ready") is True
    if not ready and not allow_partial:
        raise ValueError(
            f"{source} Silver is partial; use --allow-partial only for an exploratory example"
        )
    return record


def read_silver(spark, output, source, day, record):
    """The source's tables for the day. Every row must come from the marker's run."""
    # A Silver rerun between the marker check and this read fails here.
    F = functions()
    _, partition, _, tables = SOURCES[source]
    frames = {}
    for table in tables:
        folder = output / "silver" / table / f"{partition}={day}"
        if not (folder / "_SUCCESS").exists():
            raise ValueError(f"Silver partition is not complete: {folder}")
        # basePath: the partition column only exists in the folder name.
        frame = spark.read.option("basePath", str(output / "silver" / table)).parquet(str(folder))
        other_run = ~F.col("_silver_run_id").eqNullSafe(F.lit(record["run_id"]))
        audit = frame.agg(
            F.count("*").alias("rows"),
            F.sum(other_run.cast("long")).alias("wrong_run"),
        ).first()
        if audit["wrong_run"] or audit["rows"] != record["tables"][table]["rows"]:
            raise ValueError(f"{table}: rows disagree with the published Silver run")
        frames[table] = frame
    return frames


# --- Shared definitions ---


def count_if(condition, name):
    """Rows where condition holds: 0, never null, for a group with none."""
    F = functions()
    return F.sum(F.when(condition, 1).otherwise(0)).alias(name)


def cancelled_call():
    F = functions()
    journey = F.coalesce(F.col("journey_cancelled"), F.lit(False))
    call = F.coalesce(F.col("call_cancelled"), F.lit(False))
    return journey | call


def reported_cancellation():
    """A journey-level cancellation also reaches planned calls that got no update of their own
    (silver.build_calls)."""
    F = functions()
    journey = F.coalesce(F.col("journey_cancelled"), F.lit(False))
    return journey | (F.col("has_realtime") & cancelled_call())


def scored_departure():
    """The one definition of a timed departure, shared by every Gold table."""
    F = functions()
    delay = F.col("departure_delay_s")
    # A terminal call can have an actual departure even where boarding is forbidden.
    boarding = F.coalesce(F.col("for_boarding"), F.lit(True))
    return (
        F.col("has_realtime")
        & ~cancelled_call()
        & boarding
        & F.col("actual_departure_at").isNotNull()
        & F.col("aimed_departure_at").isNotNull()
        & delay.isNotNull()
        & ~F.isnan(delay)
        & ~F.array_contains("dq_issue", "departure_before_arrival")
        & ~F.array_contains("dq_issue", "implausible_departure_delay")
    )


def departure_measures(scored):
    """Timed departures, late ones and the delay histogram (DELAY_BINS), all additive."""
    F = functions()
    delay = F.col("departure_delay_s")
    measures = [
        count_if(scored, "scored_departures"),
        count_if(scored & (delay >= LATE_S), "late_departures"),
    ]
    for name, low, high in DELAY_BINS:
        condition = scored
        if low is not None:
            condition = condition & (delay >= low)
        if high is not None:
            condition = condition & (delay < high)
        measures.append(count_if(condition, f"departures_{name}"))
    return measures


def local_hour(column):
    F = functions()
    return F.hour(F.from_utc_timestamp(column, "Europe/Oslo"))


# --- Line scorecard ---

# Journeys and calls are counted separately (each journey once), then joined per line.
SCORECARD_SQL = """
WITH trips AS (
    SELECT operating_date,
           coalesce(line_id, {unknown}) AS line_id,
           count(*) AS input_journeys,
           count_if(has_plan) AS planned_journeys,
           count_if(has_realtime) AS reported_journeys,
           count_if(has_plan AND has_realtime) AS reported_planned_journeys,
           count_if(has_plan AND NOT has_realtime) AS unreported_planned_journeys,
           count_if(NOT has_plan AND has_realtime) AS realtime_only_journeys,
           count_if(has_realtime AND coalesce(journey_cancelled, false))
               AS reported_cancelled_journeys,
           count_if(has_plan AND lower(service_alteration) = 'cancellation')
               AS plan_cancellation_journeys,
           count_if(has_plan AND lower(service_alteration) = 'replaced')
               AS plan_replaced_journeys
    FROM {journeys}
    GROUP BY 1, 2
),
stops AS (
    SELECT operating_date,
           coalesce(line_id, {unknown}) AS line_id,
           count(*) AS input_calls,
           count_if(has_plan) AS planned_calls,
           count_if(has_realtime) AS reported_calls,
           count_if(has_plan AND has_realtime) AS reported_planned_calls,
           count_if(has_plan AND NOT has_realtime) AS unreported_planned_calls,
           count_if(NOT has_plan AND has_realtime) AS realtime_only_calls,
           count_if(has_realtime AND cancelled) AS reported_cancelled_calls,
           count_if(size(dq_issue) > 0) AS flagged_calls,
           count_if(scored) AS scored_departures,
           count_if(scored AND departure_delay_s >= {late}) AS late_departures,
           count_if(scored AND departure_delay_s < 0) AS early_departures,
           count_if(scored AND actual_departure_equals_aimed) AS actual_equals_aimed_departures,
           percentile_approx(IF(scored, departure_delay_s, NULL), array(0.5D, 0.9D), 10000)
               AS delay_percentiles
    FROM {calls}
    GROUP BY 1, 2
),
counts AS (
    -- A line can appear in one grain only: its other counts are 0, its rates stay null.
    SELECT operating_date, line_id,
           coalesce(input_journeys, 0) AS input_journeys,
           coalesce(planned_journeys, 0) AS planned_journeys,
           coalesce(reported_journeys, 0) AS reported_journeys,
           coalesce(reported_planned_journeys, 0) AS reported_planned_journeys,
           coalesce(unreported_planned_journeys, 0) AS unreported_planned_journeys,
           coalesce(realtime_only_journeys, 0) AS realtime_only_journeys,
           coalesce(reported_cancelled_journeys, 0) AS reported_cancelled_journeys,
           coalesce(plan_cancellation_journeys, 0) AS plan_cancellation_journeys,
           coalesce(plan_replaced_journeys, 0) AS plan_replaced_journeys,
           coalesce(input_calls, 0) AS input_calls,
           coalesce(planned_calls, 0) AS planned_calls,
           coalesce(reported_calls, 0) AS reported_calls,
           coalesce(reported_planned_calls, 0) AS reported_planned_calls,
           coalesce(unreported_planned_calls, 0) AS unreported_planned_calls,
           coalesce(realtime_only_calls, 0) AS realtime_only_calls,
           coalesce(reported_cancelled_calls, 0) AS reported_cancelled_calls,
           coalesce(flagged_calls, 0) AS flagged_calls,
           coalesce(scored_departures, 0) AS scored_departures,
           coalesce(late_departures, 0) AS late_departures,
           coalesce(early_departures, 0) AS early_departures,
           coalesce(actual_equals_aimed_departures, 0) AS actual_equals_aimed_departures,
           IF(planned_journeys > 0,
              round(100D * reported_planned_journeys / planned_journeys, 2), NULL)
               AS journey_reporting_pct,
           IF(planned_calls > 0,
              round(100D * reported_planned_calls / planned_calls, 2), NULL)
               AS call_reporting_pct,
           IF(scored_departures > 0,
              round(100D * late_departures / scored_departures, 2), NULL)
               AS late_departure_pct,
           delay_percentiles[0] AS median_departure_delay_s,
           delay_percentiles[1] AS p90_departure_delay_s
    FROM trips FULL JOIN stops USING (operating_date, line_id)
)
SELECT /*+ BROADCAST(l, o) */
       c.*,
       l.name AS line_name,
       l.public_code,
       l.operator_id,
       l.mode,
       o.name AS operator_name
FROM counts c
LEFT JOIN {lines} l ON l.operating_date = c.operating_date AND l.id = c.line_id
LEFT JOIN {operators} o ON o.operating_date = c.operating_date AND o.id = l.operator_id
"""


def build_scorecard(calls, journeys, lines, operators):
    """One row per line: reporting, cancellations and punctuality, with its labels."""
    assert_unique(lines, ["operating_date", "id"], "Gold line reference")
    assert_unique(operators, ["operating_date", "id"], "Gold operator reference")
    # The shared rules as columns, so SQL counts the same departures as the other tables.
    calls = (
        calls.withColumn("scored", scored_departure())
        .withColumn("cancelled", cancelled_call())
    )
    return calls.sparkSession.sql(
        SCORECARD_SQL,
        calls=calls,
        journeys=journeys,
        lines=lines,
        operators=operators,
        unknown=UNKNOWN_LINE,
        late=LATE_S,
    )


# --- Stop map ---


def build_hubs(quays, stop_places):
    """Quay -> hub (its stop place, or that stop place's parent). Returns (mapping, hub labels)."""
    # The parent joins e.g. a bus terminal and its railway station into one transfer node.
    F = functions()
    assert_unique(quays, ["operating_date", "id"], "Gold quay reference")
    assert_unique(stop_places, ["operating_date", "id"], "Gold stop place reference")
    parents = stop_places.select(F.col("id").alias("stop_place_id"), "parent_stop_place_id")
    quay_hub = (
        quays.select(F.col("id").alias("quay_id"), "stop_place_id")
        .join(parents, "stop_place_id", "left")
        .select("quay_id", F.coalesce("parent_stop_place_id", "stop_place_id").alias("hub_id"))
        .filter(F.col("hub_id").isNotNull())
    )
    labels = stop_places.select(
        F.col("id").alias("hub_id"),
        F.col("name").alias("hub_name"),
        F.col("latitude").alias("hub_latitude"),
        F.col("longitude").alias("hub_longitude"),
    )
    return quay_hub, labels


def build_stops(calls, quay_hub, hub_labels):
    """Departures per hub and local hour of the aimed departure, for the map."""
    F = functions()
    scored = scored_departure()
    at = F.coalesce("aimed_departure_at", "planned_departure_at")
    boarding = F.coalesce(F.col("for_boarding"), F.lit(True))
    cancelled = reported_cancellation() & boarding & at.isNotNull()
    return (
        calls.filter(scored | cancelled)
        .join(F.broadcast(quay_hub), "quay_id", "left")
        .withColumn("hub_id", F.coalesce("hub_id", F.lit(UNKNOWN_HUB)))
        .withColumn("hour", local_hour(at))
        .groupBy(*KEYS["stop_reliability"])
        .agg(*departure_measures(scored), count_if(cancelled, "cancelled_departures"))
        .join(F.broadcast(hub_labels), "hub_id", "left")
    )


# --- Transfers ---


def hub_calls(calls, quay_hub):
    """Calls with their hub and their journey's first and last call and hub. Endpoints come from
    all calls, so an unmapped first or last quay stays an (unknown) endpoint."""
    F = functions()
    calls = (
        calls.join(F.broadcast(quay_hub), "quay_id", "left")
        .withColumn("hub_id", F.coalesce("hub_id", F.lit(UNKNOWN_HUB)))
    )
    ends = calls.groupBy("journey_id").agg(
        F.min("call_order").alias("first_order"),
        F.max("call_order").alias("last_order"),
        F.min_by("hub_id", "call_order").alias("origin_id"),
        F.max_by("hub_id", "call_order").alias("destination_id"),
    )
    return calls.join(ends, "journey_id")


def build_line_stops(calls, quay_hub, hub_labels):
    """Arrival and departure delays per hub, line, trip end and local hour, as minute histograms.
    The dashboard estimates a transfer between two nearby hubs from them, which Gold does not pair.
    Arrivals carry their journey's origin, departures its destination. Departures are counted
    exactly as in stop_reliability, so the two reconcile."""
    F = functions()
    calls = (
        hub_calls(calls, quay_hub)
        .withColumn("line_id", F.coalesce("line_id", F.lit(UNKNOWN_LINE)))
    )
    arrived = (
        F.col("has_realtime")
        & ~cancelled_call()
        & F.col("arrival_delay_s").isNotNull()
        & ~F.isnan("arrival_delay_s")
        & ~F.array_contains("dq_issue", "implausible_arrival_delay")
    )
    sides = []
    for direction, allowed, end, timed in (
        ("arrival", "for_alighting", "origin_id", arrived),
        ("departure", "for_boarding", "destination_id", scored_departure()),
    ):
        at = F.coalesce(f"aimed_{direction}_at", f"planned_{direction}_at")
        usable = at.isNotNull() & F.coalesce(F.col(allowed), F.lit(True))
        sides.append(calls.filter(usable).select(
            "operating_date",
            "hub_id",
            "line_id",
            F.lit(direction).alias("direction"),
            F.col(end).alias("end_id"),
            local_hour(at).alias("hour"),
            reported_cancellation().alias("_cancelled"),
            F.when(timed, F.col(f"{direction}_delay_s")).alias("_delay"),
        ))
    # greatest/least skip nulls, so an unknown delay must stay null before clipping.
    floored = F.floor(F.col("_delay") / 60)
    clipped = F.least(F.greatest(floored, F.lit(DELAY_LOW)), F.lit(DELAY_HIGH))
    minute = F.when(F.col("_delay").isNotNull(), clipped)
    rows = sides[0].unionByName(sides[1]).withColumn("_minute", minute)
    histogram = F.array(*[
        F.sum(F.when(F.col("_minute") == m, 1).otherwise(0))
        for m in range(DELAY_LOW, DELAY_HIGH + 1)
    ])
    result = rows.groupBy(*KEYS["line_stop_delays"]).agg(
        F.count("*").alias("calls"),
        count_if(F.col("_cancelled"), "cancelled_calls"),
        histogram.alias("delay_counts"),
    )
    end_names = hub_labels.select(
        F.col("hub_id").alias("end_id"), F.col("hub_name").alias("end_name")
    )
    return result.join(F.broadcast(end_names), "end_id", "left")


def build_transfers(calls, quay_hub, hub_labels):
    """Observed connections: how the planned gap between an arrival and a departure changed.

    Each arrival at a hub is paired with the first departure of every other line and
    destination aimed 0-15 min later. One row per hub, line pair and local arrival hour.
    """
    F = functions()
    calls = hub_calls(calls.filter(F.col("line_id").isNotNull()), quay_hub)

    def side(prefix, kind, line, end, keep, flags):
        # One side of a connection: the arrivals ("a") or the departures ("b").
        aimed = F.coalesce(f"aimed_{kind}_at", f"planned_{kind}_at")
        trusted = ~F.arrays_overlap("dq_issue", F.array(*map(F.lit, flags)))
        # Fractional seconds kept: only the gap change is floored to whole minutes.
        delay = F.col(f"actual_{kind}_at").cast("double") - aimed.cast("double")
        usable = keep & aimed.isNotNull() & (F.col("hub_id") != UNKNOWN_HUB)
        return calls.filter(usable).select(
            "operating_date",
            "hub_id",
            F.col("line_id").alias(line),
            F.col(end[0]).alias(end[1]),
            F.col("journey_id").alias(f"{prefix}_journey"),
            F.col("call_order").alias(f"{prefix}_order"),
            aimed.cast("long").alias(f"{prefix}_at"),
            cancelled_call().alias(f"{prefix}_cancelled"),
            F.when(trusted, delay).alias(f"{prefix}_delay"),
        )

    can_alight = F.coalesce(F.col("for_alighting"), F.lit(True))
    can_board = F.coalesce(F.col("for_boarding"), F.lit(True))
    arrivals = side(
        "a", "arrival", "from_line_id", ("origin_id", "from_origin_id"),
        keep=(F.col("call_order") > F.col("first_order")) & can_alight,
        flags=["implausible_arrival_delay"],
    )
    departures = side(
        "b", "departure", "to_line_id", ("destination_id", "to_destination_id"),
        keep=(F.col("call_order") < F.col("last_order")) & can_board,
        flags=["implausible_departure_delay", "departure_before_arrival"],
    )

    # Equi-join on 15-minute buckets: a departure up to 15 min after an arrival in bucket k
    # lies in bucket k or k+1.
    bucket = F.floor(F.col("a_at") / MAX_SLACK_S)
    arrivals = arrivals.withColumn("_bucket", F.explode(F.array(bucket, bucket + 1)))
    departures = departures.withColumn("_bucket", F.floor(F.col("b_at") / MAX_SLACK_S))
    within_slack = (F.col("b_at") - F.col("a_at")).between(0, MAX_SLACK_S)
    other_line = F.col("from_line_id") != F.col("to_line_id")
    pairs = (
        arrivals.join(departures, ["operating_date", "hub_id", "_bucket"])
        .filter(within_slack & other_line)
    )

    # Keep only the first departure per arrival and (line, destination).
    arrival = ["a_journey", "a_order", "to_line_id", "to_destination_id"]
    rest = [c for c in pairs.columns if c not in arrival]
    first = F.min_by(F.struct(*rest), F.struct("b_at", "b_journey"))
    pairs = pairs.groupBy(*arrival).agg(first.alias("_first")).select(*arrival, "_first.*")

    # Gap change = departure delay - arrival delay, in whole minutes, clipped.
    gone = F.col("a_cancelled") | F.col("b_cancelled")
    known = ~gone & F.col("a_delay").isNotNull() & F.col("b_delay").isNotNull()
    minutes = F.floor((F.col("b_delay") - F.col("a_delay")) / 60)
    change = F.least(F.greatest(minutes, F.lit(GAP_LOW)), F.lit(GAP_HIGH))
    pairs = (
        pairs.withColumn("_change", F.when(known, change))
        .withColumn("hour", local_hour(F.col("a_at").cast("timestamp")))
    )

    histogram = F.array(*[
        F.sum(F.when(F.col("_change") == m, 1).otherwise(0))
        for m in range(GAP_LOW, GAP_HIGH + 1)
    ])
    result = pairs.groupBy(*KEYS["transfer_reliability"]).agg(
        F.count("*").alias("connections"),
        count_if(gone, "cancelled_connections"),
        count_if(~gone & ~known, "unknown_connections"),
        histogram.alias("gap_change_counts"),
    )
    names = hub_labels.select("hub_id", "hub_name")
    origin_names = names.toDF("from_origin_id", "from_origin_name")
    destination_names = names.toDF("to_destination_id", "to_destination_name")
    return (
        result.join(F.broadcast(origin_names), "from_origin_id", "left")
        .join(F.broadcast(destination_names), "to_destination_id", "left")
    )


# --- Delay between stops ---


def build_segments(calls, quay_hub, hub_labels):
    """Delay gained between consecutive calls of a journey: the arrival delay at the next call
    minus the departure delay here, so dwell time stays out. Negative is delay recovered. One row
    per line, hub pair and local hour of the aimed departure; pairs within one hub or with an
    unknown hub, which the map cannot draw, are left out."""
    F = functions()
    from pyspark.sql import Window

    def delay(kind, flags):
        flagged = F.arrays_overlap("dq_issue", F.array(*map(F.lit, flags)))
        trusted = F.col("has_realtime") & ~cancelled_call() & ~flagged
        late = F.col(f"actual_{kind}_at").cast("double") - F.col(f"aimed_{kind}_at").cast("double")
        return F.when(trusted, late)

    following = Window.partitionBy("journey_id").orderBy("call_order")
    rows = (
        calls.join(F.broadcast(quay_hub), "quay_id", "left")
        .select(
            "operating_date",
            "journey_id",
            "call_order",
            "hub_id",
            F.coalesce("line_id", F.lit(UNKNOWN_LINE)).alias("line_id"),
            F.coalesce("aimed_departure_at", "planned_departure_at").alias("_at"),
            delay("departure", ["implausible_departure_delay", "departure_before_arrival"])
            .alias("_departure"),
            delay("arrival", ["implausible_arrival_delay"]).alias("_arrival"),
        )
        .withColumn("to_hub_id", F.lead("hub_id").over(following))
        .withColumn("_next_arrival", F.lead("_arrival").over(following))
        .filter(F.col("_at").isNotNull() & (F.col("hub_id") != F.col("to_hub_id")))
        .withColumnRenamed("hub_id", "from_hub_id")
        .withColumn("hour", local_hour("_at"))
    )
    known = F.col("_departure").isNotNull() & F.col("_next_arrival").isNotNull()
    gained = F.col("_next_arrival") - F.col("_departure")
    change = F.least(F.greatest(gained, F.lit(-MAX_CHANGE_S)), F.lit(MAX_CHANGE_S))
    rows = rows.withColumn("_change", F.when(known, change))
    result = rows.groupBy(*KEYS["segment_delay"]).agg(
        F.count("*").alias("segments"),
        count_if(known, "timed_segments"),
        F.coalesce(F.sum("_change"), F.lit(0.0)).alias("delay_change_s"),
        count_if(F.col("_change") >= 60, "gained_1_min_plus"),
        count_if(F.col("_change") <= -60, "recovered_1_min_plus"),
    )
    starts = hub_labels.toDF("from_hub_id", "from_name", "from_latitude", "from_longitude")
    ends = hub_labels.toDF("to_hub_id", "to_name", "to_latitude", "to_longitude")
    return (
        result.join(F.broadcast(starts), "from_hub_id", "left")
        .join(F.broadcast(ends), "to_hub_id", "left")
    )


# --- Road conditions ---


def build_conditions(calls, roads, day):
    """Each timed departure with the road conditions at its quay and aimed time.

    DATEX covers one UTC day: departures outside it, or without a usable nearby
    measurement, get 'unknown'.
    """
    F = functions()
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    departures = calls.filter(scored_departure()).select(
        "operating_date", "journey_id", "call_order", "quay_id", "departure_delay_s",
        F.coalesce("mode", F.lit("unknown")).alias("mode"),
        F.col("aimed_departure_at").alias("at"),
    )
    call = ["journey_id", "call_order"]

    def flagged(issue):
        return F.array_contains("dq_issue", issue)

    # Weather: the quay's nearest usable station within 10 km, in the same UTC hour.
    stuck_rain = F.array_contains("stuck_quantities", "precipitation_mm_h")
    stuck = roads["weather_site"].select(F.col("id").alias("site_id"), stuck_rain.alias("_stuck"))
    good_rain = ~flagged("suspect_precipitation") & ~F.coalesce("_stuck", F.lit(False))
    good_surface = ~flagged("suspect_road_temperature")
    hourly = (
        roads["road_weather"].filter(~flagged("stale_measurement"))
        .join(F.broadcast(stuck), "site_id", "left")
        .groupBy("site_id", F.date_trunc("hour", "measurement_at").alias("_hour"))
        .agg(
            F.max(F.when(good_rain, F.col("precipitation_mm_h"))).alias("_precipitation"),
            F.min(F.when(good_surface, F.col("road_surface_temperature_c"))).alias("_surface"),
        )
    )
    sites = (
        roads["quay_weather_site"].filter(F.col("distance_m") <= WEATHER_MAX_M)
        .select("quay_id", "site_id")
    )

    # Situations: the worst active record linked to the quay (within 500 m in Silver).
    severity = F.when(F.col("record_type").isin(*INCIDENTS), 2).otherwise(1)
    situations = (
        roads["quay_road_situation"]
        .join(roads["road_situation_interval"], ["record_id", "version", "version_at"])
        .select("quay_id", "start_at", "end_at", severity.alias("_severity"))
    )
    during = (F.col("at") >= F.col("start_at")) & (F.col("at") < F.col("end_at"))
    active = (
        departures.select(*call, "quay_id", "at")
        .join(F.broadcast(situations), "quay_id")
        .filter(during)
        .groupBy(*call)
        .agg(F.max("_severity").alias("_situation"))
    )

    # Traffic: the most congested measured section within 500 m, in the departure's 5-minute
    # period. NPRA periods start 10 ms late (00:05:00.010), so both sides use whole periods.
    def period(column):
        return F.floor(F.col(column).cast("long") / 300).alias("_period")

    clean = F.col("has_measurement") & (F.size("dq_issue") == 0)
    periods = roads["road_travel_time"].select(
        "section_id",
        period("period_start_at"),
        F.when(clean, F.col("congestion_ratio")).alias("_ratio"),
    )
    quay_sections = roads["quay_road_section"].select("quay_id", "section_id")
    traffic = (
        departures.select(*call, "quay_id", period("at"))
        .join(F.broadcast(quay_sections), "quay_id")
        .join(F.broadcast(periods), ["section_id", "_period"], "left")
        .groupBy(*call)
        .agg(F.max("_ratio").alias("_ratio"), F.count("*").alias("_sections"))
    )

    rows = (
        departures.join(F.broadcast(sites), "quay_id", "left")
        .withColumn("_hour", F.date_trunc("hour", "at"))
        .join(F.broadcast(hourly), ["site_id", "_hour"], "left")
        .join(active, call, "left")
        .join(traffic, call, "left")
    )

    # Each departure's condition levels.
    outside = (F.col("at") < F.lit(start)) | (F.col("at") >= F.lit(start + timedelta(days=1)))
    rain, surface, ratio = F.col("_precipitation"), F.col("_surface"), F.col("_ratio")
    precipitation = (
        F.when(outside | rain.isNull(), "unknown")
        .when(rain < 0.1, "dry")
        .when(rain < 1, "light")
        .otherwise("heavy")
    )
    road_surface = (
        F.when(outside | surface.isNull(), "unknown")
        .when(surface <= 0, "freezing")
        .otherwise("above_zero")
    )
    road_situation = (
        F.when(outside, "unknown")
        .when(F.col("_situation") == 2, "incident")
        .when(F.col("_situation") == 1, "roadworks")
        .otherwise("none")
    )
    traffic_level = (
        F.when(outside, "unknown")
        .when(F.col("_sections").isNull(), "no_section")
        .when(ratio.isNull(), "unmeasured")
        .when(ratio < 1.2, "free_flow")
        .when(ratio < 1.5, "slow")
        .otherwise("congested")
    )
    return (
        rows.withColumn("hour", local_hour("at"))
        .withColumn("precipitation", precipitation)
        .withColumn("road_surface", road_surface)
        .withColumn("road_situation", road_situation)
        .withColumn("traffic", traffic_level)
        .groupBy(*KEYS["departure_conditions"])
        .agg(*departure_measures(F.lit(True)))
    )


# --- Checks and the report ---

# The run's totals, and whether the map and the conditions split exactly the scorecard's
# timed and late departures. Sums are 0, not null, for an empty table.
TOTALS_SQL = """
WITH line AS (
    SELECT coalesce(sum(input_journeys), 0) AS input_journeys,
           coalesce(sum(input_calls), 0) AS input_calls,
           coalesce(sum(planned_journeys), 0) AS planned_journeys,
           coalesce(sum(reported_journeys), 0) AS reported_journeys,
           coalesce(sum(scored_departures), 0) AS scored_departures,
           coalesce(sum(late_departures), 0) AS late_departures,
           coalesce(sum(actual_equals_aimed_departures), 0) AS actual_equals_aimed_departures
    FROM {line_scorecard}
),
transfer AS (
    SELECT coalesce(sum(connections), 0) AS transfer_connections,
           coalesce(sum(cancelled_connections), 0) AS cancelled_connections,
           coalesce(sum(unknown_connections), 0) AS unknown_connections
    FROM {transfer_reliability}
),
stop AS (
    SELECT coalesce(sum(scored_departures), 0) AS scored,
           coalesce(sum(late_departures), 0) AS late,
           coalesce(sum(cancelled_departures), 0) AS cancelled
    FROM {stop_reliability}
),
conditions AS (
    SELECT coalesce(sum(scored_departures), 0) AS scored,
           coalesce(sum(late_departures), 0) AS late
    FROM {departure_conditions}
),
line_stop AS (
    SELECT coalesce(sum(aggregate(delay_counts, 0L, (n, c) -> n + c)), 0) AS scored,
           coalesce(sum(cancelled_calls), 0) AS cancelled
    FROM {line_stop_delays} WHERE direction = 'departure'
),
segment AS (
    SELECT coalesce(sum(segments), 0) AS segments,
           coalesce(sum(timed_segments), 0) AS timed_segments
    FROM {segment_delay}
)
SELECT line.*,
       transfer.*,
       segment.*,
       stop.scored = line.scored_departures AND stop.late = line.late_departures
           AS stop_reliability_reconciles,
       conditions.scored = line.scored_departures AND conditions.late = line.late_departures
           AS departure_conditions_reconciles,
       line_stop.scored = line.scored_departures AND line_stop.cancelled = stop.cancelled
           AS line_stop_delays_reconciles
FROM line, transfer, segment, stop, conditions, line_stop
"""

REPORT_COLUMNS = (
    "Line", "Operator", "Planned journeys", "Reported planned journeys",
    "Unreported planned journeys", "Scored departures", "Late ≥3 min", "Median delay (min)",
    "P90 delay (min)", "Reported cancelled journeys",
)

REPORT_NOTES = (
    "Late means actual departure minus SIRI aimed departure ≥180 seconds. "
    "Early departures are counted separately in the CSV.",
    "Delay statistics use non-cancelled reported calls with usable actual/aimed departures, "
    "boarding not forbidden, and no departure-before-arrival issue.",
    "Missing actuals are excluded from delay statistics; "
    "a zero denominator produces a blank rate, never 0%.",
    "Journey counts and stop-call counts are aggregated separately. "
    "Each journey is counted once regardless of its number of stops.",
    "Unreported journeys are unknown. Reported cancellations, timetable cancellations and "
    "timetable replacements are separate columns in the CSV; do not add them together.",
    "{equal:,} scored departures have actual equal to aimed. "
    "This is source-quality evidence, not proof of punctuality.",
    "Labels are from the same operating day's Silver reference snapshots. "
    "No raw data is parsed by Gold.",
)


def report_markdown(record, rows):
    """A short Markdown preview of the line scorecard, for reading the run without Spark."""
    totals = record["totals"]
    if record["ready"]:
        banner = "Complete Silver input."
    else:
        banner = (
            "**PARTIAL SAMPLE — not a complete-day reliability result. "
            "Missing reports reflect this bounded input.**"
        )
    text = [
        f"# Line reliability example — {record['operating_date']}",
        "",
        banner,
        "",
        f"Source Silver run: `{record['silver_run_id']}`. Gold run: `{record['run_id']}`.",
        "",
        f"{len(rows):,} lines; {totals['planned_journeys']:,} planned journeys; "
        f"{totals['reported_journeys']:,} reported journeys; "
        f"{totals['scored_departures']:,} scored departures.",
        "",
        "Showing up to 25 lines with real-time reports, ordered by scored departures. "
        "The CSV and Parquet contain every line.",
        "",
        "| " + " | ".join(REPORT_COLUMNS) + " |",
        "|---|---|" + "---:|" * (len(REPORT_COLUMNS) - 2),
    ]

    def cell(value):
        return str(value).replace("|", "\\|").replace("\n", " ").replace("<", "&lt;")

    def number(value, scale=1, suffix=""):
        return "—" if value is None else f"{value / scale:.1f}{suffix}"

    def busiest_first(r):
        return (-r["scored_departures"], -r["reported_calls"], r["line_id"])

    reported = [r for r in rows if r["reported_journeys"] or r["reported_calls"]]
    for r in sorted(reported, key=busiest_first)[:25]:
        name = " ".join(str(r[c]) for c in ("public_code", "line_name") if r[c]) or r["line_id"]
        values = [
            cell(name),
            cell(r["operator_name"] or "Unknown"),
            r["planned_journeys"],
            r["reported_planned_journeys"],
            r["unreported_planned_journeys"],
            r["scored_departures"],
            number(r["late_departure_pct"], suffix="%"),
            number(r["median_departure_delay_s"], 60),
            number(r["p90_departure_delay_s"], 60),
            r["reported_cancelled_journeys"],
        ]
        text.append("| " + " | ".join(map(str, values)) + " |")
    if not reported:
        text += ["", "No real-time reports in this input."]

    equal = totals["actual_equals_aimed_departures"]
    text += ["", "Interpretation:", ""]
    text += ["- " + note.format(equal=equal) for note in REPORT_NOTES]
    text += [""]
    return "\n".join(text)


def check_totals(totals, entur, has_conditions=True):
    """Refuse publication unless Gold reconciles to Silver and its tables to each other."""
    silver_tables = entur["tables"]
    if (
        totals["input_journeys"] != silver_tables["journey"]["rows"]
        or totals["input_calls"] != silver_tables["stop_call"]["rows"]
    ):
        raise ValueError("Gold totals do not reconcile to Silver; refusing publication")
    checked = ["stop_reliability", "line_stop_delays"]
    if has_conditions:
        checked.append("departure_conditions")
    else:
        totals.pop("departure_conditions_reconciles", None)
    for table in checked:
        if not totals.pop(f"{table}_reconciles"):
            raise ValueError(
                f"{table} does not reconcile to the line scorecard; refusing publication"
            )


def write_preview(stage, day, scorecard, rows, record):
    """scorecard.csv and report.md next to the Parquet, in _preview (Spark skips _ folders)."""
    preview = stage / "line_scorecard" / f"operating_date={day}" / "_preview"
    preview.mkdir()
    with (preview / "scorecard.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=scorecard.columns)
        writer.writeheader()
        writer.writerows(rows)
    (preview / "report.md").write_text(report_markdown(record, rows))


# --- The run ---


def gold_spark(output):
    scratch = Path(output).parent / "spark-scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    return create_spark("DAT535-Gold", settings={
        "spark.master": "local[3]",
        "spark.driver.memory": "3g",
        "spark.sql.shuffle.partitions": "16",
        "spark.local.dir": str(scratch),
    })


def run(output_dir, day, spark=None, allow_partial=False):
    """Gold for one day: build every table in staging, check, then publish. Returns the record."""
    F = functions()
    output = Path(output_dir)
    entur = source_record(output, "entur", day, allow_partial)
    datex = source_record(output, "datex", day, allow_partial, optional=True)
    root = output / "gold"
    rid = run_id()
    stage = root / "_staging" / (rid + "-" + uuid.uuid4().hex[:8])
    history = root / "_runs" / f"operating_date={day}" / f"{rid}.json"
    owned_spark = spark is None
    started = time.monotonic()
    present = [s for s in (entur, datex) if s is not None]
    ready = all(s["status"] == "complete" and s["ready"] is True for s in present)
    datex_tables = set(SOURCES["datex"][3])
    has_datex = datex is not None and datex_tables <= set(datex.get("tables", {}))
    skipped = [] if has_datex else ["departure_conditions"]
    record: dict = dict(
        run_id=rid,
        operating_date=str(day),
        schema_version=3,
        status="running",
        ready=False,
        started_at=iso(utcnow()),
        silver_run_id=entur["run_id"],
        silver_status=entur["status"],
        datex_run_id=datex["run_id"] if has_datex else None,
        datex_status=datex["status"] if datex is not None else "missing",
        skipped_tables=skipped,
        late_seconds=LATE_S,
        max_transfer_slack_s=MAX_SLACK_S,
        max_segment_change_s=MAX_CHANGE_S,
        weather_max_m=WEATHER_MAX_M,
        tables={},
    )
    try:
        if spark is None:
            spark = gold_spark(output)

        # 1. Build the Entur tables, plus road conditions when DATEX Silver has all required tables.
        silver = read_silver(spark, output, "entur", day, entur)
        calls = silver["stop_call"]
        quay_hub, hub_labels = build_hubs(silver["quay"], silver["stop_place"])
        products = {
            "line_scorecard": build_scorecard(
                calls, silver["journey"], silver["line"], silver["operator"]
            ),
            "stop_reliability": build_stops(calls, quay_hub, hub_labels),
            "transfer_reliability": build_transfers(calls, quay_hub, hub_labels),
            "line_stop_delays": build_line_stops(calls, quay_hub, hub_labels),
            "segment_delay": build_segments(calls, quay_hub, hub_labels),
        }
        if has_datex:
            roads = read_silver(spark, output, "datex", day, datex)
            products["departure_conditions"] = build_conditions(calls, roads, day)

        # 2. Write each to staging and check its key.
        written = {}
        for table, frame in products.items():
            partition = stage / table / f"operating_date={day}"
            (
                frame.withColumn("_gold_run_id", F.lit(rid))
                .withColumn("_source_silver_run_id", F.lit(record["silver_run_id"]))
                .withColumn("_source_datex_run_id", F.lit(record["datex_run_id"]).cast("string"))
                .write.parquet(str(partition))
            )
            written[table] = spark.read.parquet(str(partition))
            assert_unique(written[table], KEYS[table], f"Gold {table}")
            record["tables"][table] = dict(
                rows=written[table].count(),
                partition=f"operating_date={day}",
                parquet_bytes=sum(p.stat().st_size for p in partition.glob("*.parquet")),
            )

        # 3. Reconcile.
        sql_tables = dict(written)
        if "departure_conditions" not in sql_tables:
            sql_tables["departure_conditions"] = spark.createDataFrame(
                [], "scored_departures long, late_departures long"
            )
        totals = spark.sql(TOTALS_SQL, **sql_tables).first().asDict()
        check_totals(totals, entur, has_conditions="departure_conditions" in written)

        # Only small aggregates reach the driver, never stop-call facts.
        rows = [r.asDict() for r in written["line_scorecard"].orderBy("line_id").collect()]
        # How much of the day each road condition level could describe.
        levels = record["condition_levels"] = {}
        if "departure_conditions" in written:
            levels.update({dim: {} for dim in KEYS["departure_conditions"][3:]})
            for row in written["departure_conditions"].collect():
                for dim, counts in levels.items():
                    counts[row[dim]] = counts.get(row[dim], 0) + row["scored_departures"]

        # 4. Publish.
        record.update(
            totals=totals,
            status="complete" if ready else "partial",
            ready=ready,
            finished_at=iso(utcnow()),
            elapsed_s=round(time.monotonic() - started, 3),
        )
        write_preview(stage, day, written["line_scorecard"], rows, record)
        atomic_json(history, record)
        publish(stage, root, record, day)
        return record
    except BaseException as error:
        record.update(status="failed", ready=False, error=f"{type(error).__name__}: {error}")
        atomic_json(history, record)
        raise
    finally:
        if stage.exists():
            shutil.rmtree(stage)
        if owned_spark and spark is not None:
            spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    default_output = Path(os.environ.get("DAT535_OUTPUT_DIR", "output"))
    default_day = (utcnow() - timedelta(days=2)).date()
    parser.add_argument(
        "--date",
        type=Date.fromisoformat,
        default=default_day,
        help="Norwegian operating day, YYYY-MM-DD (default: two days ago)",
    )
    parser.add_argument("--output-dir", type=Path, default=default_output)
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="create an explicitly non-ready exploration result",
    )
    args = parser.parse_args(argv)
    try:
        result = run(args.output_dir, args.date, allow_partial=args.allow_partial)
    except (OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
