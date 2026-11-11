#!/usr/bin/env python3
"""Daily Gold over published Silver: line scorecard, stop map, transfer chances, road conditions.

One run reads one Norwegian operating day of Entur Silver and the same UTC observation day of
DATEX Silver. Every count is additive, so consumers sum days instead of averaging rates.
Partial Silver requires --allow-partial and produces an explicitly non-ready result.
Run under the same flock as Silver; see docs/pipeline/gold-example.md.
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

# Source: (marker folder under silver/, partition column, marker schema version, tables read).
SOURCES = {
    "entur": (Path("_runs"), "operating_date", 2,
              ("stop_call", "journey", "line", "operator", "quay", "stop_place")),
    "datex": (Path("_runs") / "datex", "observation_date", 1,
              ("road_weather", "weather_site", "road_situation_interval", "road_travel_time",
               "quay_weather_site", "quay_road_situation", "quay_road_section")),
}
KEYS = {
    "line_scorecard": ["operating_date", "line_id"],
    "stop_reliability": ["operating_date", "hub_id", "hour"],
    "transfer_reliability": ["operating_date", "hub_id", "from_line_id", "from_origin_id",
                             "to_line_id", "to_destination_id", "hour"],
    "departure_conditions": ["operating_date", "mode", "hour", "precipitation", "road_surface",
                             "road_situation", "traffic"],
}
UNKNOWN_LINE, UNKNOWN_HUB = "__unknown_line__", "__unknown_hub__"
LATE_S = 180
# Departure delay bins in seconds, [low, high). Late (>= 3 min) is the sum of the last three.
DELAY_BINS = (("early", None, -60), ("on_time", -60, 60), ("1_3_min", 60, 180),
              ("3_5_min", 180, 300), ("5_10_min", 300, 600), ("10_min_plus", 600, None))
MAX_SLACK_S = 15 * 60
# Gap change in whole minutes, floor((departure delay - arrival delay) / 60), clipped. A
# connection planned with b spare minutes holds when the change is >= -b, exact for b in 0..10.
GAP_LOW, GAP_HIGH = -11, 10
WEATHER_MAX_M = 10_000
# DATEX record types that describe something going wrong on the road, not planned management.
INCIDENTS = ("Accident", "AnimalPresenceObstruction", "EnvironmentalObstruction", "GeneralObstruction",
             "InfrastructureDamageObstruction", "VehicleObstruction", "RoadsideAssistance",
             "PoorEnvironmentConditions", "WeatherRelatedRoadConditions",
             "NonWeatherRelatedRoadConditions", "EquipmentOrSystemFault")


def source_record(output, source, day, allow_partial=False):
    runs, partition, schema, _ = SOURCES[source]
    path = Path(output) / "silver" / runs / f"{partition}={day}" / "latest.json"
    record = json.loads(path.read_text())
    if record.get("schema_version") != schema or record.get(partition) != str(day):
        raise ValueError(f"{source} Silver marker has an unsupported schema or wrong date")
    if record.get("status") not in {"complete", "partial"}:
        raise ValueError(f"{source} Silver is not published: refusing a failed or publishing marker")
    if not (record.get("status") == "complete" and record.get("ready") is True) and not allow_partial:
        raise ValueError(f"{source} Silver is partial; use --allow-partial only for an exploratory example")
    return record


def read_silver(spark, output, source, day, record):
    """Every row must come from the marker's run: a rerun between check and read fails here."""
    F = functions()
    _, partition, _, tables = SOURCES[source]
    frames = {}
    for table in tables:
        folder = output / "silver" / table / f"{partition}={day}"
        if not (folder / "_SUCCESS").exists():
            raise ValueError(f"Silver partition is not complete: {folder}")
        # Partition keys can live only in directory names. Keep the table root for discovery.
        frame = spark.read.option("basePath", str(output / "silver" / table)).parquet(str(folder))
        audit = frame.agg(F.count("*").alias("rows"), F.sum(
            (~F.col("_silver_run_id").eqNullSafe(F.lit(record["run_id"]))).cast("long")).alias("wrong_run")).first()
        if audit["wrong_run"] or audit["rows"] != record["tables"][table]["rows"]:
            raise ValueError(f"{table}: rows disagree with the published Silver run")
        frames[table] = frame
    return frames


def count_if(condition, name):
    F = functions()
    return F.sum(F.when(condition, 1).otherwise(0)).alias(name)


def cancelled_call():
    F = functions()
    return F.coalesce(F.col("journey_cancelled"), F.lit(False)) | F.coalesce(F.col("call_cancelled"), F.lit(False))


def scored_departure():
    """The one definition of a timed departure, shared by every Gold table."""
    F = functions()
    delay = F.col("departure_delay_s")
    # A terminal call can have an actual departure even when boarding is forbidden.
    # Missing coordinates/labels do not make otherwise usable timing evidence invalid.
    return F.col("has_realtime") & ~cancelled_call() & F.coalesce(F.col("for_boarding"), F.lit(True)) & \
        F.col("actual_departure_at").isNotNull() & F.col("aimed_departure_at").isNotNull() & \
        delay.isNotNull() & ~F.isnan(delay) & \
        ~F.array_contains("dq_issue", "departure_before_arrival") & \
        ~F.array_contains("dq_issue", "implausible_departure_delay")


def departure_measures(scored):
    F = functions()
    delay = F.col("departure_delay_s")
    measures = [count_if(scored, "scored_departures"), count_if(scored & (delay >= LATE_S), "late_departures")]
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


def build_scorecard(calls, journeys, lines, operators):
    """Aggregate each fact grain separately, then attach daily reference labels."""
    F = functions()
    key = KEYS["line_scorecard"]

    def grouped(frame):
        return frame.withColumn("line_id", F.coalesce("line_id", F.lit(UNKNOWN_LINE)))

    def percent(numerator, denominator):
        return F.when(F.col(denominator) > 0, F.round(100.0 * F.col(numerator) / F.col(denominator), 2))

    assert_unique(lines, ["operating_date", "id"], "Gold line reference")
    assert_unique(operators, ["operating_date", "id"], "Gold operator reference")
    planned, reported = F.col("has_plan"), F.col("has_realtime")
    cancelled = F.coalesce(F.col("journey_cancelled"), F.lit(False))
    trip_counts = grouped(journeys).groupBy(*key).agg(
        F.count("*").alias("input_journeys"),
        count_if(planned, "planned_journeys"),
        count_if(reported, "reported_journeys"),
        count_if(planned & reported, "reported_planned_journeys"),
        count_if(planned & ~reported, "unreported_planned_journeys"),
        count_if(~planned & reported, "realtime_only_journeys"),
        count_if(reported & cancelled, "reported_cancelled_journeys"),
        count_if(planned & (F.lower("service_alteration") == "cancellation"), "plan_cancellation_journeys"),
        count_if(planned & (F.lower("service_alteration") == "replaced"), "plan_replaced_journeys"),
    )

    scored, delay = scored_departure(), F.col("departure_delay_s")
    call_counts = grouped(calls).groupBy(*key).agg(
        F.count("*").alias("input_calls"),
        count_if(planned, "planned_calls"),
        count_if(reported, "reported_calls"),
        count_if(planned & reported, "reported_planned_calls"),
        count_if(planned & ~reported, "unreported_planned_calls"),
        count_if(~planned & reported, "realtime_only_calls"),
        count_if(reported & cancelled_call(), "reported_cancelled_calls"),
        count_if(F.size("dq_issue") > 0, "flagged_calls"),
        count_if(scored, "scored_departures"),
        count_if(scored & (delay >= LATE_S), "late_departures"),
        count_if(scored & (delay < 0), "early_departures"),
        count_if(scored & F.col("actual_departure_equals_aimed"), "actual_equals_aimed_departures"),
        F.percentile_approx(F.when(scored, delay), [0.5, 0.9], 10000).alias("_delay_percentiles"),
    )
    count_columns = [c for c in trip_counts.columns + call_counts.columns if c not in key + ["_delay_percentiles"]]
    result = trip_counts.join(call_counts, key, "full").fillna(0, subset=count_columns)
    result = result.withColumn("journey_reporting_pct", percent("reported_planned_journeys", "planned_journeys")) \
        .withColumn("call_reporting_pct", percent("reported_planned_calls", "planned_calls")) \
        .withColumn("late_departure_pct", percent("late_departures", "scored_departures")) \
        .withColumn("median_departure_delay_s", F.col("_delay_percentiles")[0]) \
        .withColumn("p90_departure_delay_s", F.col("_delay_percentiles")[1]).drop("_delay_percentiles")
    labels = lines.select("operating_date", F.col("id").alias("line_id"),
                          F.col("name").alias("line_name"), "public_code", "operator_id", "mode")
    operator_labels = operators.select("operating_date", F.col("id").alias("operator_id"),
                                       F.col("name").alias("operator_name"))
    return result.join(F.broadcast(labels), key, "left") \
        .join(F.broadcast(operator_labels), ["operating_date", "operator_id"], "left")


def build_hubs(quays, stop_places):
    """Quay -> hub: its NSR stop place, or that stop place's multimodal parent, so a bus terminal
    and the railway station it belongs to are one transfer node. Labels come from the hub's row."""
    F = functions()
    assert_unique(quays, ["operating_date", "id"], "Gold quay reference")
    assert_unique(stop_places, ["operating_date", "id"], "Gold stop place reference")
    parents = stop_places.select(F.col("id").alias("stop_place_id"), "parent_stop_place_id")
    quay_hub = quays.select(F.col("id").alias("quay_id"), "stop_place_id").join(parents, "stop_place_id", "left") \
        .select("quay_id", F.coalesce("parent_stop_place_id", "stop_place_id").alias("hub_id"))
    labels = stop_places.select(F.col("id").alias("hub_id"), F.col("name").alias("hub_name"),
                                F.col("latitude").alias("hub_latitude"), F.col("longitude").alias("hub_longitude"))
    return quay_hub.filter(F.col("hub_id").isNotNull()), labels


def build_stops(calls, quay_hub, hub_labels):
    """Departures per hub and local hour of the aimed departure, for the map."""
    F = functions()
    scored = scored_departure()
    at = F.coalesce("aimed_departure_at", "planned_departure_at")
    cancelled = F.col("has_realtime") & cancelled_call() & F.coalesce(F.col("for_boarding"), F.lit(True)) & at.isNotNull()
    rows = calls.filter(scored | cancelled).join(F.broadcast(quay_hub), "quay_id", "left") \
        .withColumn("hub_id", F.coalesce("hub_id", F.lit(UNKNOWN_HUB))).withColumn("hour", local_hour(at))
    return rows.groupBy(*KEYS["stop_reliability"]).agg(*departure_measures(scored), count_if(cancelled, "cancelled_departures")) \
        .join(F.broadcast(hub_labels), "hub_id", "left")


def build_transfers(calls, quay_hub, hub_labels):
    """Observed connections. Each arrival at a hub is paired with the first departure of every
    other line and destination aimed 0-15 min later. One row per hub, line pair and local arrival
    hour, with a histogram of how the planned gap changed: departure delay minus arrival delay."""
    F = functions()
    from pyspark.sql import Window
    calls = calls.filter(F.col("line_id").isNotNull()).join(F.broadcast(quay_hub), "quay_id")
    ends = calls.groupBy("journey_id").agg(
        F.min("call_order").alias("first_order"), F.max("call_order").alias("last_order"),
        F.min_by("hub_id", "call_order").alias("origin_id"), F.max_by("hub_id", "call_order").alias("destination_id"))
    calls = calls.join(ends, "journey_id")

    def side(prefix, kind, line, end, keep, flags):
        aimed = F.coalesce(f"aimed_{kind}_at", f"planned_{kind}_at")
        trusted = ~F.arrays_overlap("dq_issue", F.array(*map(F.lit, flags)))
        return calls.filter(keep & aimed.isNotNull()).select(
            "operating_date", "hub_id", F.col("line_id").alias(line), F.col(end[0]).alias(end[1]),
            F.col("journey_id").alias(f"{prefix}_journey"), F.col("call_order").alias(f"{prefix}_order"),
            aimed.cast("long").alias(f"{prefix}_at"), cancelled_call().alias(f"{prefix}_cancelled"),
            F.when(trusted, F.col(f"actual_{kind}_at").cast("long") - aimed.cast("long")).alias(f"{prefix}_delay"))

    arrivals = side("a", "arrival", "from_line_id", ("origin_id", "from_origin_id"),
                    (F.col("call_order") > F.col("first_order")) & F.coalesce(F.col("for_alighting"), F.lit(True)),
                    ["implausible_arrival_delay"])
    departures = side("b", "departure", "to_line_id", ("destination_id", "to_destination_id"),
                      (F.col("call_order") < F.col("last_order")) & F.coalesce(F.col("for_boarding"), F.lit(True)),
                      ["implausible_departure_delay", "departure_before_arrival"])
    # Equi-join on 15-minute buckets: a departure up to 15 min after an arrival in bucket k lies in k or k+1.
    bucket = F.floor(F.col("a_at") / MAX_SLACK_S)
    arrivals = arrivals.withColumn("_bucket", F.explode(F.array(bucket, bucket + 1)))
    departures = departures.withColumn("_bucket", F.floor(F.col("b_at") / MAX_SLACK_S))
    pairs = arrivals.join(departures, ["operating_date", "hub_id", "_bucket"]).filter(
        (F.col("b_at") - F.col("a_at")).between(0, MAX_SLACK_S) & (F.col("from_line_id") != F.col("to_line_id")))
    first = Window.partitionBy("a_journey", "a_order", "to_line_id", "to_destination_id").orderBy("b_at", "b_journey")
    pairs = pairs.withColumn("_rank", F.row_number().over(first)).filter(F.col("_rank") == 1)

    gone = F.col("a_cancelled") | F.col("b_cancelled")
    known = ~gone & F.col("a_delay").isNotNull() & F.col("b_delay").isNotNull()
    change = F.least(F.greatest(F.floor((F.col("b_delay") - F.col("a_delay")) / 60), F.lit(GAP_LOW)), F.lit(GAP_HIGH))
    pairs = pairs.withColumn("_change", F.when(known, change)) \
        .withColumn("hour", local_hour(F.col("a_at").cast("timestamp")))
    result = pairs.groupBy(*KEYS["transfer_reliability"]).agg(
        F.count("*").alias("connections"),
        count_if(gone, "cancelled_connections"),
        count_if(~gone & ~known, "unknown_connections"),
        F.array(*[F.sum(F.when(F.col("_change") == m, 1).otherwise(0)) for m in range(GAP_LOW, GAP_HIGH + 1)])
        .alias("gap_change_counts"))
    names = hub_labels.select("hub_id", "hub_name")
    return result \
        .join(F.broadcast(names.toDF("from_origin_id", "from_origin_name")), "from_origin_id", "left") \
        .join(F.broadcast(names.toDF("to_destination_id", "to_destination_name")), "to_destination_id", "left")


def build_conditions(calls, roads, day):
    """Each timed departure with the road conditions at its quay and aimed time. DATEX covers one
    UTC day; departures outside it, or without a usable nearby measurement, are 'unknown'."""
    F = functions()
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    departures = calls.filter(scored_departure()).select(
        "operating_date", "journey_id", "call_order", "quay_id", "departure_delay_s",
        F.coalesce("mode", F.lit("unknown")).alias("mode"), F.col("aimed_departure_at").alias("at"))
    call = ["journey_id", "call_order"]

    # Weather: the quay's nearest usable station within 10 km, measured in the same UTC hour.
    flagged = lambda issue: F.array_contains("dq_issue", issue)
    stuck = roads["weather_site"].select(F.col("id").alias("site_id"),
                                         F.array_contains("stuck_quantities", "precipitation_mm_h").alias("_stuck"))
    hourly = roads["road_weather"].filter(~flagged("stale_measurement")).join(F.broadcast(stuck), "site_id", "left") \
        .groupBy("site_id", F.date_trunc("hour", "measurement_at").alias("_hour")).agg(
            F.max(F.when(~flagged("suspect_precipitation") & ~F.coalesce("_stuck", F.lit(False)),
                         F.col("precipitation_mm_h"))).alias("_precipitation"),
            F.min(F.when(~flagged("suspect_road_temperature"), F.col("road_surface_temperature_c"))).alias("_surface"))
    sites = roads["quay_weather_site"].filter(F.col("distance_m") <= WEATHER_MAX_M).select("quay_id", "site_id")

    # Situations: the worst active record linked to the quay (within 500 m in Silver).
    situations = roads["quay_road_situation"].join(
        roads["road_situation_interval"], ["record_id", "version", "version_at"]).select(
        "quay_id", "start_at", "end_at", F.when(F.col("record_type").isin(*INCIDENTS), 2).otherwise(1).alias("_severity"))
    active = departures.select(*call, "quay_id", "at").join(F.broadcast(situations), "quay_id") \
        .filter((F.col("at") >= F.col("start_at")) & (F.col("at") < F.col("end_at"))) \
        .groupBy(*call).agg(F.max("_severity").alias("_situation"))

    # Traffic: the most congested measured section within 500 m in the departure's 5-minute period.
    # NPRA periods start 10 ms after the boundary (00:05:00.010), so both sides use whole periods.
    period = lambda column: F.floor(F.col(column).cast("long") / 300).alias("_period")
    periods = roads["road_travel_time"].select("section_id", period("period_start_at"), F.when(
        F.col("has_measurement") & (F.size("dq_issue") == 0), F.col("congestion_ratio")).alias("_ratio"))
    traffic = departures.select(*call, "quay_id", period("at")) \
        .join(F.broadcast(roads["quay_road_section"].select("quay_id", "section_id")), "quay_id") \
        .join(F.broadcast(periods), ["section_id", "_period"], "left") \
        .groupBy(*call).agg(F.max("_ratio").alias("_ratio"), F.count("*").alias("_sections"))

    rows = departures.join(F.broadcast(sites), "quay_id", "left").withColumn("_hour", F.date_trunc("hour", "at")) \
        .join(F.broadcast(hourly), ["site_id", "_hour"], "left").join(active, call, "left").join(traffic, call, "left")
    outside = (F.col("at") < F.lit(start)) | (F.col("at") >= F.lit(start + timedelta(days=1)))
    rain, surface, ratio = F.col("_precipitation"), F.col("_surface"), F.col("_ratio")
    rows = rows.withColumn("hour", local_hour("at")) \
        .withColumn("precipitation", F.when(outside | rain.isNull(), "unknown").when(rain < 0.1, "dry")
                    .when(rain < 1, "light").otherwise("heavy")) \
        .withColumn("road_surface", F.when(outside | surface.isNull(), "unknown").when(surface <= 0, "freezing")
                    .otherwise("above_zero")) \
        .withColumn("road_situation", F.when(outside, "unknown").when(F.col("_situation") == 2, "incident")
                    .when(F.col("_situation") == 1, "roadworks").otherwise("none")) \
        .withColumn("traffic", F.when(outside, "unknown").when(F.col("_sections").isNull(), "no_section")
                    .when(ratio.isNull(), "unmeasured").when(ratio < 1.2, "free_flow").when(ratio < 1.5, "slow")
                    .otherwise("congested"))
    return rows.groupBy(*KEYS["departure_conditions"]).agg(*departure_measures(F.lit(True)))


def report_markdown(record, rows):
    totals = record["totals"]
    banner = "Complete Silver input." if record["ready"] else \
        "**PARTIAL SAMPLE — not a complete-day reliability result. Missing reports reflect this bounded input.**"
    text = [f"# Line reliability example — {record['operating_date']}", "", banner, "",
            f"Source Silver run: `{record['silver_run_id']}`. Gold run: `{record['run_id']}`.", "",
            f"{len(rows):,} lines; {totals['planned_journeys']:,} planned journeys; "
            f"{totals['reported_journeys']:,} reported journeys; {totals['scored_departures']:,} scored departures.", "",
            "Showing up to 25 lines with real-time reports, ordered by scored departures. The CSV and Parquet contain every line.", "",
            "| Line | Operator | Planned journeys | Reported planned journeys | Unreported planned journeys | Scored departures | Late ≥3 min | Median delay (min) | P90 delay (min) | Reported cancelled journeys |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]

    def cell(value):
        return str(value).replace("|", "\\|").replace("\n", " ").replace("<", "&lt;")

    def number(value, scale=1, suffix=""):
        return "—" if value is None else f"{value / scale:.1f}{suffix}"

    visible = sorted((r for r in rows if r["reported_journeys"] or r["reported_calls"]),
                     key=lambda r: (-r["scored_departures"], -r["reported_calls"], r["line_id"]))[:25]
    for r in visible:
        name = " ".join(str(r[c]) for c in ("public_code", "line_name") if r[c]) or r["line_id"]
        values = [cell(name), cell(r["operator_name"] or "Unknown"), r["planned_journeys"],
                  r["reported_planned_journeys"], r["unreported_planned_journeys"], r["scored_departures"],
                  number(r["late_departure_pct"], suffix="%"), number(r["median_departure_delay_s"], 60),
                  number(r["p90_departure_delay_s"], 60), r["reported_cancelled_journeys"]]
        text.append("| " + " | ".join(map(str, values)) + " |")
    if not visible:
        text += ["", "No real-time reports in this input."]
    text += ["", "Interpretation:", "",
             "- Late means actual departure minus SIRI aimed departure ≥180 seconds. Early departures are counted separately in the CSV.",
             "- Delay statistics use non-cancelled reported calls with usable actual/aimed departures, boarding not forbidden, and no departure-before-arrival issue.",
             "- Missing actuals are excluded from delay statistics; a zero denominator produces a blank rate, never 0%.",
             "- Journey counts and stop-call counts are aggregated separately. Each journey is counted once regardless of its number of stops.",
             "- Unreported journeys are unknown. Reported cancellations, timetable cancellations and timetable replacements are separate columns in the CSV; do not add them together.",
             f"- {totals['actual_equals_aimed_departures']:,} scored departures have actual equal to aimed. This is source-quality evidence, not proof of punctuality.",
             "- Labels are from the same operating day's Silver reference snapshots. No raw data is parsed by Gold.", ""]
    return "\n".join(text)


def run(output_dir, day, spark=None, allow_partial=False):
    F = functions()
    output = Path(output_dir)
    sources = {name: source_record(output, name, day, allow_partial) for name in SOURCES}
    root = output / "gold"
    rid = run_id()
    stage = root / "_staging" / (rid + "-" + uuid.uuid4().hex[:8])
    history = root / "_runs" / f"operating_date={day}" / f"{rid}.json"
    owned_spark = spark is None
    started = time.monotonic()
    ready = all(s["status"] == "complete" and s["ready"] is True for s in sources.values())
    record: dict = dict(run_id=rid, operating_date=str(day), schema_version=2, status="running", ready=False,
                  started_at=iso(utcnow()), silver_run_id=sources["entur"]["run_id"],
                  silver_status=sources["entur"]["status"], datex_run_id=sources["datex"]["run_id"],
                  datex_status=sources["datex"]["status"], late_seconds=LATE_S,
                  max_transfer_slack_s=MAX_SLACK_S, weather_max_m=WEATHER_MAX_M, tables={})
    try:
        if spark is None:
            scratch = output.parent / "spark-scratch"
            scratch.mkdir(parents=True, exist_ok=True)
            spark = create_spark("DAT535-Gold", settings={
                "spark.master": "local[3]", "spark.driver.memory": "3g",
                "spark.sql.shuffle.partitions": "16", "spark.local.dir": str(scratch)})
        silver = read_silver(spark, output, "entur", day, sources["entur"])
        roads = read_silver(spark, output, "datex", day, sources["datex"])
        calls = silver["stop_call"]
        quay_hub, hub_labels = build_hubs(silver["quay"], silver["stop_place"])
        products = {
            "line_scorecard": build_scorecard(calls, silver["journey"], silver["line"], silver["operator"]),
            "stop_reliability": build_stops(calls, quay_hub, hub_labels),
            "transfer_reliability": build_transfers(calls, quay_hub, hub_labels),
            "departure_conditions": build_conditions(calls, roads, day),
        }
        written = {}
        for table, frame in products.items():
            partition = stage / table / f"operating_date={day}"
            frame.withColumn("_gold_run_id", F.lit(rid)).withColumn("_source_silver_run_id", F.lit(record["silver_run_id"])) \
                .withColumn("_source_datex_run_id", F.lit(record["datex_run_id"])).write.parquet(str(partition))
            written[table] = spark.read.parquet(str(partition))
            assert_unique(written[table], KEYS[table], f"Gold {table}")
            record["tables"][table] = dict(rows=written[table].count(), partition=f"operating_date={day}",
                                           parquet_bytes=sum(p.stat().st_size for p in partition.glob("*.parquet")))
        # Only the small line/day aggregate reaches the driver, never stop-call facts.
        rows = [r.asDict() for r in written["line_scorecard"].orderBy("line_id").collect()]
        totals = {c: sum(r[c] for r in rows) for c in (
            "input_journeys", "input_calls", "planned_journeys", "reported_journeys", "scored_departures",
            "late_departures", "actual_equals_aimed_departures")}
        if totals["input_journeys"] != sources["entur"]["tables"]["journey"]["rows"] or \
                totals["input_calls"] != sources["entur"]["tables"]["stop_call"]["rows"]:
            raise ValueError("Gold totals do not reconcile to Silver; refusing publication")
        # The map and the conditions split the same timed departures as the scorecard.
        for table in ("stop_reliability", "departure_conditions"):
            check = written[table].agg(F.sum("scored_departures"), F.sum("late_departures")).first()
            if (check[0] or 0, check[1] or 0) != (totals["scored_departures"], totals["late_departures"]):
                raise ValueError(f"{table} does not reconcile to the line scorecard; refusing publication")
        transfers = written["transfer_reliability"].agg(F.sum("connections"), F.sum("cancelled_connections"),
                                                        F.sum("unknown_connections")).first()
        totals.update(transfer_connections=transfers[0] or 0, cancelled_connections=transfers[1] or 0,
                      unknown_connections=transfers[2] or 0)
        # How much of the day each road condition could describe, for the run record.
        levels = written["departure_conditions"].collect()
        record["condition_levels"] = {dim: {} for dim in KEYS["departure_conditions"][3:]}
        for row in levels:
            for dim, counts in record["condition_levels"].items():
                counts[row[dim]] = counts.get(row[dim], 0) + row["scored_departures"]
        record.update(totals=totals, status="complete" if ready else "partial", ready=ready,
                      finished_at=iso(utcnow()), elapsed_s=round(time.monotonic() - started, 3))
        # Spark ignores underscore directories when discovering Parquet files.
        preview = stage / "line_scorecard" / f"operating_date={day}" / "_preview"
        preview.mkdir()
        with (preview / "scorecard.csv").open("w", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=written["line_scorecard"].columns)
            writer.writeheader()
            writer.writerows(rows)
        (preview / "report.md").write_text(report_markdown(record, rows))
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
    parser.add_argument("--date", type=Date.fromisoformat, required=True, help="Norwegian operating day")
    parser.add_argument("--output-dir", type=Path, default=Path(os.environ.get("DAT535_OUTPUT_DIR", "output")))
    parser.add_argument("--allow-partial", action="store_true", help="create an explicitly non-ready exploration result")
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
