#!/usr/bin/env python3
"""Entur Silver: archived plan/stops + Bronze ET, SX and vehicles -> reusable facts.

--date is a Norwegian operating day (default: two days ago, so the next UTC day's Bronze is done).
Contracts, readiness and reruns: docs/pipeline/silver.md.
"""

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
import time
import uuid
import zipfile
from datetime import date as Date, timedelta
from functools import partial
from pathlib import Path

from bronze import DATASETS, HOURS, create_spark as bronze_spark, iso, run_id, utcnow
from silver_parse import arrow_decoder, decode_partition, schema
from silver_reference import (
    archived_file,
    nsr_partition,
    plan_members,
    plan_partition,
    reference_schema,
)

log = logging.getLogger("silver")

JOURNEY_KEY = ["operating_date", "journey_id"]
CALL_KEY = JOURNEY_KEY + ["call_order"]
LEAD_MINUTES = [1, 5, 10, 20, 30]

# 2: entity_index became a long. Days written with 1 are rebuilt before reading both.
SCHEMA_VERSION = 2

# Quarantine reasons that mean the source broke its contract (not just one bad value).
CONTRACT_ISSUES = frozenset({
    "bronze_corrupt",
    "bronze_incomplete",
    "bronze_wrong_endpoint",
    "invalid_vehicle_shape",
    "unknown_message_shape",
    "malformed_payload",
    "source_delivery_error",
    "graphql_error",
    "graphql_partial_error",
})


def functions():
    from pyspark.sql import functions as F

    return F


def empty_issues():
    return functions().array().cast("array<string>")


def issues(df, *rules):
    """Append the name of every (name, condition) rule that holds to dq_issue. Rows are kept."""
    F = functions()
    hits = F.array(*[F.when(condition, F.lit(name)) for name, condition in rules])
    new = F.filter(hits.cast("array<string>"), lambda x: x.isNotNull())
    old = F.coalesce(F.col("dq_issue"), empty_issues())
    return df.withColumn("dq_issue", F.sort_array(F.array_distinct(F.concat(old, new))))


def assert_unique(df, key, name):
    F = functions()
    any_null = F.exists(F.array(*[F.col(k).isNull() for k in key]), lambda x: x)
    if df.filter(any_null).limit(1).count():
        raise ValueError(f"{name}: null primary key {key}")
    if df.groupBy(*key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{name}: duplicate primary key {key}; refusing a multiplying join")


def current_reference(df):
    """Keep each id's highest (numeric) version. Ties stay, so assert_unique catches conflicts."""
    from pyspark.sql import Window

    F = functions()
    # Numeric, so version 10 beats 9.
    version = F.col("version").cast("decimal(38,0)")
    newest = F.max(version).over(Window.partitionBy("id"))
    return (
        df.withColumn("_version", version)
        .withColumn("_newest", newest)
        .filter(F.col("_version").eqNullSafe(F.col("_newest")))
        .drop("_version", "_newest")
    )


def latest(df, key, actual_first=False):
    """One row per key: the newest by producer time, then arrival time, then tie-breakers."""
    F = functions()
    order = []
    if actual_first:
        arrived = F.col("actual_arrival_at").isNotNull()
        departed = F.col("actual_departure_at").isNotNull()
        order.append(arrived | departed)
    if "recorded_at" in df.columns:
        order.append(F.col("recorded_at"))
    order += [F.col("received_at"), F.col("payload_sha256"), F.col("entity_index")]

    # max_by compares the struct field by field; null is smallest, so a missing time loses.
    values = [c for c in df.columns if c not in key]
    newest = F.max_by(F.struct(*values), F.struct(*order))
    return (
        df.groupBy(*key)
        .agg(newest.alias("_latest"))
        .select(*key, "_latest.*")
        .select(*df.columns)
    )


def normalize_journeys(df, aliases):
    """Map dated aliases to the plan's journey id and give every version an operating day."""
    F = functions()
    df = df.withColumn("journey_id", F.coalesce("journey_id", "dated_journey_id"))
    mapped = aliases.select(
        F.col("dated_journey_id").alias("_alias"),
        F.col("journey_id").alias("_mapped_journey"),
        F.col("operating_date").alias("_mapped_date"),
    )
    # Vehicles may carry both ids; ET may carry the dated alias in journey_id itself.
    alias_ref = F.coalesce(F.col("dated_journey_id"), F.col("journey_id"))
    joined = df.join(mapped, alias_ref == mapped._alias, "left")

    date, mapped_date = F.col("operating_date"), F.col("_mapped_date")
    journey, mapped_journey = F.col("journey_id"), F.col("_mapped_journey")
    date_conflict = date.isNotNull() & mapped_date.isNotNull() & (date != mapped_date)
    journey_conflict = (
        mapped_journey.isNotNull()
        & journey.isNotNull()
        & (journey != F.col("_alias"))
        & (journey != mapped_journey)
    )
    joined = issues(
        joined,
        ("conflicting_operating_date", date_conflict),
        ("conflicting_journey_id", journey_conflict),
    )
    joined = (
        joined.withColumn("source_journey_id", journey)
        .withColumn("journey_id", F.coalesce("_mapped_journey", "journey_id"))
    )

    if "derived_operating_date" not in joined.columns:
        joined = joined.withColumn("operating_date", F.coalesce("operating_date", "_mapped_date"))
        return joined.drop("_alias", "_mapped_journey", "_mapped_date")

    # Day precedence: explicit, then the plan's alias, then what the id or first call says.
    # A dated id of another day resolves to that day; the caller filters it out.
    derived = date.isNull() & mapped_date.isNull() & F.col("derived_operating_date").isNotNull()
    date_source = (
        F.when(date.isNotNull(), "framed")
        .when(mapped_date.isNotNull(), "alias")
        .otherwise(F.col("derived_date_source"))
    )
    joined = (
        issues(joined, ("operating_date_derived", derived))
        .withColumn("date_source", date_source)
        .withColumn(
            "operating_date",
            F.coalesce("operating_date", "_mapped_date", "derived_operating_date"),
        )
        .drop("derived_operating_date", "derived_date_source")
    )
    return joined.drop("_alias", "_mapped_journey", "_mapped_date")


def merge_plan(rt, plan, key):
    """Full outer join of real-time and plan at the same grain: keeps the planned denominator."""
    F = functions()
    shared = ("line_id", "operator_id", "direction_id", "mode", "quay_id")
    common = [c for c in shared if c in plan.columns]
    plan_only = [c for c in plan.columns if c not in key + common + ["dq_issue", "source_member"]]
    planned = plan.select(
        *key,
        *[F.col(c).alias("plan_" + c) for c in common],
        *plan_only,
        F.col("dq_issue").alias("plan_dq_issue"),
        F.col("source_member").alias("plan_source_member"),
        F.lit(True).alias("has_plan"),
    )

    merged = rt.withColumn("has_realtime", F.lit(True)).join(planned, key, "full")
    # Real-time wins; the plan fills in what it left empty.
    for c in common:
        merged = merged.withColumn(c, F.coalesce(F.col(c), F.col("plan_" + c)))

    both_issues = F.array_distinct(F.concat(
        F.coalesce(F.col("dq_issue"), empty_issues()),
        F.coalesce(F.col("plan_dq_issue"), empty_issues()),
    ))
    status = (
        F.when(~F.col("has_realtime"), "no_realtime")
        .when(~F.col("has_plan"), "realtime_only")
        .otherwise("matched")
    )
    return (
        merged.withColumn("dq_issue", both_issues)
        .drop("plan_dq_issue")
        .fillna(False, subset=["has_plan", "has_realtime"])
        .withColumn("realtime_status", status)
    )


def conform(df, quays, lines):
    """Flag quay and line ids the day's reference snapshot does not know. The row stays."""
    F = functions()
    checks = (("quay_id", quays, "unknown_quay"), ("line_id", lines, "unknown_line"))
    for column, reference, issue in checks:
        if column not in df.columns:
            continue
        known = reference.select(F.col("id").alias(column), F.lit(True).alias("_known"))
        df = df.join(F.broadcast(known), column, "left")
        unknown = F.col(column).isNotNull() & F.col("_known").isNull()
        df = issues(df, (issue, unknown)).drop("_known")
    return df


def build_journeys(versions, plan):
    columns = JOURNEY_KEY + [
        "line_id", "operator_id", "direction_id", "mode", "codespace", "vehicle_id",
        "source_mode", "date_source", "journey_cancelled", "extra_journey", "monitored",
        "recorded_at", "received_at", "payload_sha256", "record_sha256", "source_file",
        "bronze_run_id", "source_journey_id", "entity_index", "dq_issue",
    ]
    journeys = versions.filter(functions().col("kind") == "journey").select(*columns)
    return merge_plan(latest(journeys, JOURNEY_KEY), plan, JOURNEY_KEY)


def build_calls(versions, plan, journeys):
    F = functions()
    attributes = [
        "line_id", "operator_id", "direction_id", "mode", "source_mode", "codespace",
        "vehicle_id", "monitored", "date_source",
    ]
    calls = (
        versions.filter(F.col("kind") == "call")
        .drop("kind", "entity_json", "source_date", "journey_cancelled", "extra_journey")
        .drop(*attributes)
    )
    final = latest(calls, CALL_KEY, actual_first=True)
    final = final.withColumn("reported_quay_id", F.col("quay_id"))

    # Call versions carry no journey attributes (silver_parse.parse_et); the journey's latest do.
    reported = journeys.filter(F.col("has_realtime"))
    journey_attributes = [c for c in attributes if c in journeys.columns]
    final = final.join(reported.select(*JOURNEY_KEY, *journey_attributes), JOURNEY_KEY, "left")
    final = merge_plan(final, plan, CALL_KEY)

    # A cancellation-only journey message may have no calls: its flag still reaches every call.
    journey_flags = journeys.select(
        *JOURNEY_KEY,
        "journey_cancelled",
        "extra_journey",
        F.col("has_realtime").alias("journey_has_realtime"),
    )
    final = final.join(journey_flags, JOURNEY_KEY, "left")

    quay_changed = (
        F.col("has_plan")
        & F.col("has_realtime")
        & (F.col("reported_quay_id") != F.col("plan_quay_id"))
    )
    final = final.withColumn("quay_changed", quay_changed)

    # Delay = actual - ET's aimed time (which may differ from the archived plan).
    for event in ("arrival", "departure"):
        actual, aimed = F.col(f"actual_{event}_at"), F.col(f"aimed_{event}_at")
        final = (
            final.withColumn(f"{event}_delay_s", actual.cast("double") - aimed.cast("double"))
            .withColumn(f"actual_{event}_equals_aimed", actual == aimed)
        )

    # Over 30 min early or 6 h late: batch-stamped or wrong-day actuals, not real delays.
    # Example: a whole ferry journey stamped 9 h before departure (2026-10-29).
    def implausible(delay):
        return (F.col(delay) < -1800) | (F.col(delay) > 21600)

    no_aimed = F.col("aimed_arrival_at").isNull() & F.col("aimed_departure_at").isNull()
    return issues(
        final,
        ("missing_quay", F.col("quay_id").isNull()),
        ("implausible_arrival_delay", implausible("arrival_delay_s")),
        ("implausible_departure_delay", implausible("departure_delay_s")),
        ("missing_reported_quay", F.col("has_realtime") & F.col("reported_quay_id").isNull()),
        ("nonpositive_call_order", F.col("call_order") <= 0),
        ("departure_before_arrival", F.col("actual_departure_at") < F.col("actual_arrival_at")),
        ("missing_aimed_time", F.col("has_realtime") & no_aimed),
    )


def asof_intervals(df, key, actual_column=None):
    """Give each version the span it was the best known: [available_at, next_available_at)."""
    from pyspark.sql import Window

    F = functions()
    # Known only once both the producer stamped it and we received it.
    df = (
        df.filter(F.col("recorded_at").isNotNull() & F.col("received_at").isNotNull())
        .withColumn("available_at", F.greatest("recorded_at", "received_at"))
    )
    priority = [F.col(actual_column).isNotNull().alias("actual")] if actual_column else []
    priority += [F.col("recorded_at"), F.col("received_at"), F.col("payload_sha256"),
                 F.col("entity_index")]
    df = df.withColumn("_priority", F.struct(*priority))

    # One version per moment: the best of those that arrived at the same time.
    at_time = Window.partitionBy(*key, "available_at").orderBy(F.col("_priority").desc())
    df = (
        df.withColumn("_rank", F.row_number().over(at_time))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    # Drop a late replay of older data: it must beat everything seen before it.
    knowledge = Window.partitionBy(*key).orderBy("available_at")
    seen = knowledge.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = (
        df.withColumn("_best_seen", F.max("_priority").over(seen))
        .filter(F.col("_priority") == F.col("_best_seen"))
        .drop("_priority", "_best_seen")
    )
    return df.withColumn("next_available_at", F.lead("available_at").over(knowledge))


def build_predictions(versions, final_calls, leads=LEAD_MINUTES):
    """What was known about each call at fixed lead times before its planned and actual time."""
    F = functions()

    def known_at_cutoff(state, key):
        # The version of `state` whose interval contains the target's cutoff.
        start, end = F.col(f"{state}.available_at"), F.col(f"{state}.next_available_at")
        cutoff = F.col("t.cutoff_at")
        condition = (start <= cutoff) & (end.isNull() | (cutoff < end))
        for column in key:
            condition &= F.col(f"{state}.{column}") == F.col(f"t.{column}")
        return condition

    journeys = versions.filter(F.col("kind") == "journey")
    journey_states = asof_intervals(journeys, JOURNEY_KEY).alias("j")

    parts = []
    for event in ("arrival", "departure"):
        calls = versions.filter(F.col("kind") == "call").select(
            *CALL_KEY,
            "recorded_at", "received_at", "payload_sha256", "source_file", "bronze_run_id",
            "entity_index", "dq_issue",
            F.col(f"expected_{event}_at").alias("predicted_at"),
            F.col(f"actual_{event}_at").alias("known_actual_at"),
            "call_cancelled", "journey_cancelled",
        )
        # States without a prediction stay: they end the previous state's interval.
        call_states = asof_intervals(calls, CALL_KEY, actual_column="known_actual_at").alias("v")

        # Targets: the planned and the actual time, each at every lead time.
        planned = F.coalesce(f"planned_{event}_at", f"aimed_{event}_at")
        actual = F.col(f"actual_{event}_at")
        references = F.array(
            F.struct(F.lit("planned").alias("reference"), planned.alias("target_at")),
            F.struct(F.lit("actual").alias("reference"), actual.alias("target_at")),
        )
        cutoff = F.col("target_at").cast("double") - F.col("lead_minutes") * 60
        targets = (
            final_calls.select(*CALL_KEY, F.explode(references).alias("target"))
            .select(*CALL_KEY, "target.*")
            .filter(F.col("target_at").isNotNull())
            .withColumn("lead_minutes", F.explode(F.array(*[F.lit(n) for n in leads])))
            .withColumn("cutoff_at", F.timestamp_seconds(cutoff))
            .alias("t")
        )

        # Left joins: a journey cancellation can give a state when no call update was known.
        # Keep only cutoffs before the actual, with a prediction or a cancellation.
        has_state = (
            F.col("v.predicted_at").isNotNull()
            | (F.col("v.call_cancelled") == True)
            | (F.col("j.journey_cancelled") == True)
        )
        joined = (
            targets.join(call_states, known_at_cutoff("v", CALL_KEY), "left")
            .filter(F.col("v.known_actual_at").isNull())
            .join(journey_states, known_at_cutoff("j", JOURNEY_KEY), "left")
            .filter(has_state)
        )

        dq_issue = F.sort_array(F.array_distinct(F.concat(
            F.coalesce(F.col("v.dq_issue"), empty_issues()),
            F.coalesce(F.col("j.dq_issue"), empty_issues()),
        )))
        parts.append(joined.select(
            *[F.col("t." + c).alias(c) for c in CALL_KEY],
            F.lit(event).alias("event"),
            "t.reference",
            "t.target_at",
            "t.lead_minutes",
            "t.cutoff_at",
            "v.predicted_at",
            F.greatest("v.available_at", "j.available_at").alias("available_at"),
            F.coalesce("v.recorded_at", "j.recorded_at").alias("recorded_at"),
            F.coalesce("v.received_at", "j.received_at").alias("received_at"),
            "v.call_cancelled",
            "j.journey_cancelled",
            F.col("j.recorded_at").alias("journey_recorded_at"),
            F.col("j.received_at").alias("journey_received_at"),
            F.col("j.payload_sha256").alias("journey_payload_sha256"),
            F.coalesce("v.payload_sha256", "j.payload_sha256").alias("payload_sha256"),
            F.coalesce("v.source_file", "j.source_file").alias("source_file"),
            F.coalesce("v.bronze_run_id", "j.bronze_run_id").alias("bronze_run_id"),
            dq_issue.alias("dq_issue"),
        ))
    return parts[0].unionByName(parts[1])


def quarantine_rows(df, feed):
    """Rejected rows of any kind in one shape: where they came from, why, and the row as JSON."""
    F = functions()
    row = F.struct(*[F.col(c) for c in df.columns if c != "entity_json"])
    return df.select(
        F.lit(feed).alias("feed"),
        F.col("kind").alias("record_kind"),
        "source_date", "received_at", "source_file", "payload_sha256", "record_sha256",
        "bronze_run_id", "dq_issue",
        F.to_json(row).alias("entity_json"),
    )


def stage_table(df, stage, table, day, rid, key, partition="operating_date", metadata=None,
                schema_version=SCHEMA_VERSION):
    """Write one table's partition to staging and check what was written. Returns (frame, stats)."""
    F = functions()
    path = stage / table / f"{partition}={day}"
    if partition not in df.columns:
        df = df.withColumn(partition, F.lit(day))
    df = (
        df.withColumn("_silver_run_id", F.lit(rid))
        .withColumn("_silver_schema_version", F.lit(schema_version))
    )
    for name, value in (metadata or {}).items():
        is_time = name.endswith("_available_at")
        df = df.withColumn(name, F.lit(value).cast("timestamp") if is_time else F.lit(value))
    df.write.mode("overwrite").parquet(str(path))

    # Check the written files: a unique key and every row in this run's partition.
    written = df.sparkSession.read.parquet(str(path))
    if key:
        assert_unique(written, key, table)
    elsewhere = F.col(partition).isNull() | (F.col(partition) != F.lit(day))
    stats = written.agg(
        F.count("*").alias("rows"),
        F.sum((F.size("dq_issue") > 0).cast("long")).alias("flagged_rows"),
        F.sum(elsewhere.cast("long")).alias("wrong_partition_rows"),
    ).first().asDict()
    if stats.pop("wrong_partition_rows"):
        raise ValueError(f"{table}: rows do not belong to {partition}={day}")

    stats["flagged_rows"] = stats["flagged_rows"] or 0
    issue_counts = (
        written.select(F.explode("dq_issue").alias("issue"))
        .groupBy("issue")
        .count()
        .collect()
    )
    stats["issues"] = {r["issue"]: r["count"] for r in issue_counts}
    stats["parquet_bytes"] = sum(p.stat().st_size for p in path.glob("*.parquet"))
    log.info("%s: %s", table, stats)
    return written, stats


def atomic_json(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(record, indent=2, default=str) + "\n")
    os.replace(tmp, path)


def publish(stage, root, record, day, runs=None):
    """Move staged tables into place, then write the readiness marker. Roll back on failure."""
    marker = (runs or root / "_runs" / f"operating_date={day}") / "latest.json"
    previous = marker.read_text() if marker.exists() else None
    moved, installed = [], []
    atomic_json(marker, dict(record, status="publishing", ready=False))
    try:
        for table, info in record["tables"].items():
            relative = Path(table) / info["partition"]
            source, dest = stage / relative, root / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            # The old partition waits in this run's staging directory until the marker is written.
            if dest.exists():
                backup = stage / "_previous" / relative
                backup.parent.mkdir(parents=True, exist_ok=True)
                os.replace(dest, backup)
                moved.append((dest, backup))
            os.replace(source, dest)
            installed.append(dest)
        atomic_json(marker, record)
    except BaseException:
        for dest in reversed(installed):
            shutil.rmtree(dest)
        for dest, backup in reversed(moved):
            os.replace(backup, dest)
        if previous is not None:
            atomic_json(marker, json.loads(previous))
        else:
            marker.unlink(missing_ok=True)
        raise


def audit_bronze(frame):
    """One Bronze day: (rows per _bronze_status, hours present, Bronze run ids). Narrow columns."""
    F = functions()
    audit = (
        frame.groupBy("_bronze_status", "_bronze_run_id")
        .agg(F.count("*").alias("rows"), F.collect_set("hour").alias("hours"))
        .collect()
    )
    statuses = {}
    for r in audit:
        statuses[r["_bronze_status"]] = statuses.get(r["_bronze_status"], 0) + r["rows"]
    hours = {h for r in audit for h in r["hours"]}
    run_ids = sorted({r["_bronze_run_id"] for r in audit})
    return statuses, hours, run_ids


def read_bronze(spark, output, feed, day, allow_partial, record):
    """The Bronze days a feed needs for the operating day. Returns ({day: frame}, complete)."""
    F = functions()
    # An operating day spans UTC days, so ET also needs the day before and after.
    days = [day + timedelta(days=n) for n in (-1, 0, 1)] if feed == "et" else [day]
    frames = {}
    evidence = {}
    for source_day in days:
        folder = Path(output) / "bronze" / DATASETS[feed] / f"date={source_day}"
        if not (folder / "_SUCCESS").exists():
            evidence[str(source_day)] = {"missing_partition": True, "missing_hours": HOURS}
            continue
        frame = spark.read.parquet(str(folder))
        statuses, hours, run_ids = audit_bronze(frame)
        evidence[str(source_day)] = {
            "missing_hours": sorted(set(HOURS) - hours),
            "rows": sum(statuses.values()),
            "bronze_status": statuses,
            "bronze_run_ids": run_ids,
            "parquet_bytes": sum(p.stat().st_size for p in folder.glob("*.parquet")),
        }
        frames[source_day] = frame.withColumn("source_date", F.lit(str(source_day)))

    record["inputs"][feed] = evidence
    complete = all(not entry["missing_hours"] for entry in evidence.values())
    if not complete and not allow_partial:
        raise ValueError(
            f"incomplete {feed} Bronze coverage: {evidence}; "
            "use --allow-partial only for exploration"
        )
    if not frames:
        raise FileNotFoundError(f"no completed Bronze input for {feed} near {day}")

    def source(frame):
        # A row's identity: its payload hash, else its raw line, else where it landed.
        if "_bronze_raw_line" in frame.columns:
            raw_line = F.col("_bronze_raw_line")
        else:
            raw_line = F.lit(None).cast("string")
        landed = F.to_json(F.struct("received_at", "_bronze_status", "_bronze_source_file"))
        record_sha = F.coalesce(
            "_bronze_payload_sha256", F.sha2(raw_line, 256), F.sha2(landed, 256)
        )
        return frame.withColumn("record_sha256", record_sha).select(
            "received_at", "payload", "_bronze_source_file", "_bronze_payload_sha256",
            "record_sha256", "_bronze_run_id", "_bronze_status", "source_date",
        )

    sources = {source_day: source(frame) for source_day, frame in frames.items()}
    return sources, complete


def decode_day(spark, root, stage, feed, source_day, source, evidence):
    """Typed versions of one Bronze day, decoded once and cached under silver/_decoded/.

    ET needs D-1, D and D+1 for every operating day; the cache saves parsing each day three times.
    """
    folder = root / "_decoded" / feed / f"source_date={source_day}"
    audit = evidence[str(source_day)]
    # Reuse the cache only for the same Bronze input and the same decoder source code.
    decoder_file = Path(__file__).with_name("silver_parse.py")
    stamp = dict(
        decoder=hashlib.sha256(decoder_file.read_bytes()).hexdigest(),
        bronze_run_ids=audit["bronze_run_ids"],
        rows=audit["rows"],
        parquet_bytes=audit["parquet_bytes"],
    )
    marker = folder / "_decoded.json"
    if marker.exists() and json.loads(marker.read_text()) == stamp:
        return folder

    work = stage / "_work" / f"{feed}-{source_day}"
    # Small Arrow batches: one ET message can be 22M characters (10 000 rows ~ 300 MB).
    batch_setting = "spark.sql.execution.arrow.maxRecordsPerBatch"
    previous = spark.conf.get(batch_setting)
    spark.conf.set(batch_setting, "500")
    try:
        spec = schema(feed)
        decoder = arrow_decoder(partial(decode_partition, feed=feed), spec)
        source.mapInArrow(decoder, spec).write.parquet(str(work))
    finally:
        spark.conf.set(batch_setting, previous)

    atomic_json(work / "_decoded.json", stamp)
    folder.parent.mkdir(parents=True, exist_ok=True)
    if folder.exists():
        shutil.rmtree(folder)
    os.replace(work, folder)
    return folder


def silver_spark(app, output_dir, master, shuffle_partitions):
    """Bronze's Spark settings, with scratch space on the data volume (not the small root disk)."""
    default_scratch = str(Path(output_dir).parent / "spark-scratch")
    scratch = Path(os.environ.get("DAT535_SPARK_LOCAL_DIR", default_scratch))
    scratch.mkdir(parents=True, exist_ok=True)
    return bronze_spark(app, settings={
        "spark.master": os.environ.get("SPARK_MASTER", master),
        "spark.local.dir": str(scratch),
        "spark.sql.shuffle.partitions": os.environ.get(
            "DAT535_SHUFFLE_PARTITIONS", shuffle_partitions
        ),
    })


def reference_snapshots(landing, day, record):
    """The day's archived NeTEx and NSR zips: {dataset: (snapshot day, path)}."""
    snapshots = {dataset: archived_file(landing, dataset, day) for dataset in ("netex", "nsr")}
    # Recorded first, so a failed check below still shows which files were read.
    record["references"] = {
        dataset: {
            "snapshot_date": str(snapshot_day),
            "file": str(path),
            "bytes": path.stat().st_size,
        }
        for dataset, (snapshot_day, path) in snapshots.items()
    }
    for dataset, (_, path) in snapshots.items():
        entry = record["references"][dataset]
        meta = path.with_name(path.name + ".meta.json")
        # The collector's manifest has the checksum; hash the zip only without one.
        if meta.exists():
            archived = json.loads(meta.read_text())
            if archived["bytes"] != entry["bytes"]:
                raise ValueError(f"archive size differs from collector manifest: {path}")
            entry["sha256"] = archived["sha256"]
            entry["available_at"] = archived.get("finished_at")
        else:
            with path.open("rb") as source:
                entry["sha256"] = hashlib.file_digest(source, "sha256").hexdigest()
    return snapshots


def decode_references(spark, work, netex_path, nsr_path, day):
    """The day's plan (NeTEx) and stops (NSR), one zip member per task. Returns (planned, stops)."""
    F = functions()
    sc = spark.sparkContext

    # Shared members add operators; they have no ServiceJourneys.
    members = plan_members(netex_path)
    with zipfile.ZipFile(netex_path) as z:
        members += [
            (n, None)
            for n in sorted(z.namelist())
            if n.startswith("_") and n.endswith("shared_data.xml")
        ]
    tasks = min(len(members), sc.defaultParallelism * 4)
    plan = sc.parallelize(members, tasks).mapPartitions(
        lambda it: plan_partition(it, str(netex_path), day)
    )
    spark.createDataFrame(plan, reference_schema()).write.parquet(str(work / "plan"))
    planned = spark.read.parquet(str(work / "plan"))
    if not planned.filter(F.col("kind") == "journey").limit(1).count():
        raise ValueError(f"archived timetable contains no supported journeys for {day}")

    with zipfile.ZipFile(nsr_path) as z:
        stop_members = sorted(n for n in z.namelist() if n.endswith(".xml"))
    tasks = min(len(stop_members), sc.defaultParallelism)
    stops = sc.parallelize(stop_members, tasks).mapPartitions(
        lambda it: nsr_partition(it, str(nsr_path))
    )
    spark.createDataFrame(stops, reference_schema()).write.parquet(str(work / "nsr"))
    return planned, spark.read.parquet(str(work / "nsr"))


def build_dimension(source, table, columns):
    """One reference table (quay, stop_place, line, operator) at its newest version per id."""
    F = functions()
    dim = source.filter(F.col("kind") == table).select(*columns)
    # Line members repeat identical references; a real conflict fails uniqueness instead.
    dim = current_reference(dim.drop("source_member").dropDuplicates())
    if table not in {"quay", "stop_place"}:
        return dim
    lat, lon = F.col("latitude"), F.col("longitude")
    return issues(
        dim,
        ("missing_coordinates", lat.isNull() | lon.isNull()),
        ("invalid_coordinates", ~lat.between(-90, 90) | ~lon.between(-180, 180)),
    )


def scope(parsed, feed, day, aliases):
    """Split a feed's decoded versions into (kept, [quarantine rows]). Nothing vanishes silently."""
    F = functions()
    rejected = [quarantine_rows(parsed.filter(F.col("kind") == "quarantine"), feed)]
    parsed = parsed.filter(~F.col("kind").isin("quarantine", "control"))

    if feed == "sx":
        parsed = issues(
            parsed,
            ("missing_situation_id", F.col("situation_id").isNull()),
            ("missing_participant_id", F.col("participant_id").isNull()),
        )
        valid = F.col("situation_id").isNotNull() & F.col("participant_id").isNotNull()
    else:
        if feed == "et":
            # ET of another operating day is not an error: that day's run owns it.
            this_day = (F.col("operating_date") == F.lit(day)) | F.col("operating_date").isNull()
            parsed = parsed.filter(this_day)
        parsed = normalize_journeys(parsed, aliases)
        parsed = issues(
            parsed,
            ("missing_operating_date", F.col("operating_date").isNull()),
            ("missing_journey_id", F.col("journey_id").isNull()),
        )
        if feed == "et":
            is_call = F.col("kind") == "call"
            parsed = issues(
                parsed,
                ("missing_call_order", is_call & F.col("call_order").isNull()),
                ("missing_recorded_at", F.col("recorded_at").isNull()),
            )
            valid = (
                F.col("operating_date").isNotNull()
                & F.col("journey_id").isNotNull()
                & (~is_call | F.col("call_order").isNotNull())
            )
        else:
            parsed = issues(
                parsed,
                ("missing_vehicle_id", F.col("vehicle_id").isNull()),
                ("missing_codespace", F.col("codespace").isNull()),
                ("missing_last_updated_at", F.col("last_updated_at").isNull()),
            )
            valid = (
                F.col("vehicle_id").isNotNull()
                & F.col("codespace").isNotNull()
                & F.col("last_updated_at").isNotNull()
            )

    rejected.append(quarantine_rows(parsed.filter(~valid), feed))
    parsed = parsed.filter(valid)
    if feed == "et":
        # An undated journey may have resolved to another day in normalize_journeys.
        parsed = parsed.filter(F.col("operating_date") == F.lit(day))
    return parsed, rejected


def build_vehicles(versions):
    """One row per vehicle and update time, its latest copy, with implausible values flagged."""
    F = functions()
    vehicles = latest(versions, ["codespace", "vehicle_id", "last_updated_at"])
    lat, lon = F.col("latitude"), F.col("longitude")
    return issues(
        vehicles,
        ("missing_coordinates", lat.isNull() | lon.isNull()),
        ("invalid_coordinates", ~lat.between(-90, 90) | ~lon.between(-180, 180)),
        ("invalid_bearing", ~F.col("bearing").between(0, 360)),
        ("negative_speed", F.col("speed") < 0),
        ("invalid_progress", ~F.col("progress_pct").between(0, 100)),
    )


def build_situations(versions):
    """One row per situation version, with when it was first received."""
    from pyspark.sql import Window

    F = functions()
    key = ["participant_id", "situation_id", "version_key"]
    first_received = F.min("received_at").over(Window.partitionBy(*key))
    sx = latest(versions.withColumn("first_received_at", first_received), key)
    backwards = F.exists("validity_periods", lambda p: p.end_at < p.start_at)
    return issues(sx, ("invalid_validity_period", backwards))


def call_reconciliation(record, calls):
    """How the retained call versions reduce to stop calls, for the run record."""
    F = functions()
    by_status = calls.groupBy("realtime_status").count().collect()
    coverage = {r["realtime_status"]: r["count"] for r in by_status}
    versions = record["inputs"]["et"]["retained_versions"].get("call", 0)
    reported = coverage.get("matched", 0) + coverage.get("realtime_only", 0)
    record["call_coverage"] = coverage
    record["call_reconciliation"] = {
        "input_versions": versions,
        "planned_calls": coverage.get("matched", 0) + coverage.get("no_realtime", 0),
        "reported_calls": reported,
        "duplicate_versions_removed": versions - reported,
    }

    equals_aimed = F.coalesce(F.col("actual_departure_equals_aimed"), F.lit(False))
    by_operator = calls.groupBy("operator_id").agg(
        F.count("actual_departure_at").alias("actual_departures"),
        F.sum(equals_aimed.cast("long")).alias("actual_equals_aimed"),
    )
    record["actual_equals_aimed_by_operator"] = [r.asDict() for r in by_operator.collect()]


def finish(record, complete, allow_partial, started):
    """Mark a finished run: ready only with complete input and no source contract errors."""
    record["finished_at"] = iso(utcnow())
    record["elapsed_s"] = round(time.monotonic() - started, 3)
    record["status"] = "complete" if complete and not record["contract_errors"] else "partial"
    record["ready"] = record["status"] == "complete"
    # A strict run with contract errors fails instead of publishing.
    if record["contract_errors"] and not allow_partial:
        raise ValueError(f"source contract errors: {record['contract_errors']}")


# Reference tables: (table, source, columns). quay/stop_place come from NSR, the rest from NeTEx.
DIMENSIONS = (
    ("quay", "stops", ["id", "version", "name", "stop_place_id", "latitude", "longitude",
                       "source_member", "dq_issue"]),
    ("stop_place", "stops", ["id", "version", "name", "parent_stop_place_id", "latitude",
                             "longitude", "source_member", "dq_issue"]),
    ("line", "planned", ["id", "version", "name", "operator_id", "mode", "public_code",
                         "is_flexible", "source_member", "dq_issue"]),
    ("operator", "planned", ["id", "version", "name", "source_member", "dq_issue"]),
)


def run(landing, output_dir, day, spark=None, allow_partial=False, leads=LEAD_MINUTES):
    F = functions()
    root = Path(output_dir) / "silver"
    if root.resolve().is_relative_to(Path(landing).resolve()):
        raise ValueError("Silver output must be outside the immutable landing directory")
    rid = run_id()
    started = time.monotonic()
    stage = root / "_staging" / (rid + "-" + uuid.uuid4().hex[:8])
    record = dict(
        run_id=rid,
        env=os.environ.get("DAT535_ENV", "local"),
        schema_version=SCHEMA_VERSION,
        operating_date=str(day),
        started_at=iso(utcnow()),
        status="running",
        ready=False,
        inputs={},
        tables={},
        lead_minutes=list(leads),
    )
    history = root / "_runs" / f"operating_date={day}" / f"{rid}.json"
    owned_spark = spark is None
    try:
        # 1. Inputs: reference zips and Bronze coverage.
        snapshots = reference_snapshots(landing, day, record)
        netex_day, netex_path = snapshots["netex"]
        nsr_day, nsr_path = snapshots["nsr"]
        if spark is None:
            spark = silver_spark("DAT535-Silver-Entur", output_dir, "local[4]", "32")
        for module in ("bronze.py", "silver.py", "silver_parse.py", "silver_reference.py"):
            spark.sparkContext.addPyFile(str(Path(__file__).with_name(module)))
        stage.mkdir(parents=True)

        inputs, complete = {}, True
        for feed in DATASETS:
            inputs[feed], covered = read_bronze(spark, output_dir, feed, day, allow_partial, record)
            complete &= covered
        planned, stops = decode_references(spark, stage / "_work", netex_path, nsr_path, day)

        def save(df, table, key, partition="operating_date", metadata=None):
            written, stats = stage_table(df, stage, table, day, rid, key, partition, metadata)
            stats["partition"] = f"{partition}={day}"
            record["tables"][table] = stats
            return written

        # 2. Reference tables and the plan.
        sources = {"stops": stops, "planned": planned}
        dimensions = {}
        for table, source, columns in DIMENSIONS:
            snapshot = nsr_day if source == "stops" else netex_day
            dimension = build_dimension(sources[source], table, columns)
            dimensions[table] = save(dimension, table, ["id"], metadata={"snapshot_date": snapshot})
            if table != "operator" and record["tables"][table]["rows"] == 0:
                raise ValueError(f"reference contract: no {table} entities decoded")

        plan_journeys = planned.filter(F.col("kind") == "journey")
        aliases = plan_journeys.select(
            *JOURNEY_KEY, F.explode("dated_journey_ids").alias("dated_journey_id")
        ).dropDuplicates()
        assert_unique(aliases, ["dated_journey_id"], "dated journey mapping")
        aliases = save(
            aliases.withColumn("dq_issue", empty_issues()),
            "dated_journey",
            ["dated_journey_id"],
            metadata={"snapshot_date": netex_day},
        )
        plan_journey = plan_journeys.select(
            *JOURNEY_KEY,
            "line_id", "operator_id", "direction_type", "mode", "journey_pattern_id",
            "service_alteration", "dated_journey_ids", "is_flexible", "source_member", "dq_issue",
        )
        plan_call = planned.filter(F.col("kind") == "call").select(
            *CALL_KEY,
            "line_id", "operator_id", "direction_type", "mode", "quay_id",
            "planned_arrival_at", "planned_departure_at", "earliest_departure_at",
            "latest_arrival_at", "is_flexible", "for_boarding", "for_alighting",
            "source_member", "dq_issue",
        )
        assert_unique(plan_journey, JOURNEY_KEY, "plan journeys")
        assert_unique(plan_call, CALL_KEY, "plan calls")

        # 3. Decode and scope each feed. Small feeds first: fail before hours of ET decoding.
        quarantine, versions = [], {}
        for feed in ("sx", "vehicles", "et"):
            evidence = record["inputs"][feed]
            folders = [
                str(decode_day(spark, root, stage, feed, source_day, frame, evidence))
                for source_day, frame in inputs[feed].items()
            ]
            parsed = spark.read.parquet(*folders)
            decoded = parsed.groupBy("kind").count().collect()
            evidence["decoded"] = {r["kind"]: r["count"] for r in decoded}

            retained, rejected = scope(parsed, feed, day, aliases)
            quarantine += rejected
            # Written and read back: several tables read it, so the alias join runs once.
            scoped = str(stage / "_work" / f"{feed}-scoped")
            retained.write.parquet(scoped)
            versions[feed] = spark.read.parquet(scoped)
            kept = versions[feed].groupBy("kind").count().collect()
            evidence["retained_versions"] = {r["kind"]: r["count"] for r in kept}

        # 4. Fact tables.
        metadata = {
            "netex_snapshot_date": netex_day,
            "nsr_snapshot_date": nsr_day,
            "netex_available_at": record["references"]["netex"].get("available_at"),
            "nsr_available_at": record["references"]["nsr"].get("available_at"),
        }
        quays, lines = dimensions["quay"], dimensions["line"]

        journeys = conform(build_journeys(versions["et"], plan_journey), quays, lines)
        journeys = save(journeys, "journey", JOURNEY_KEY, metadata=metadata)

        calls = conform(build_calls(versions["et"], plan_call, journeys), quays, lines)
        calls = save(calls, "stop_call", CALL_KEY, metadata=metadata)
        call_reconciliation(record, calls)

        predictions = build_predictions(versions["et"], calls, leads)
        prediction_key = CALL_KEY + ["event", "reference", "lead_minutes"]
        save(predictions, "call_prediction", prediction_key, metadata=metadata)

        vehicles = conform(build_vehicles(versions["vehicles"]), quays, lines)
        save(
            vehicles.drop("kind", "entity_json", "source_date"),
            "vehicle_position",
            ["codespace", "vehicle_id", "last_updated_at"],
            partition="observation_date",
            metadata=metadata,
        )
        save(
            build_situations(versions["sx"]).drop("kind", "entity_json", "source_date"),
            "sx_situation",
            ["participant_id", "situation_id", "version_key"],
            partition="observation_date",
        )

        rejected = quarantine[0]
        for frame in quarantine[1:]:
            rejected = rejected.unionByName(frame)
        save(rejected, "quarantine", None)

        # 5. Verdict, then publish.
        quarantine_issues = record["tables"]["quarantine"]["issues"]
        record["contract_errors"] = {
            k: n for k, n in quarantine_issues.items() if k in CONTRACT_ISSUES
        }
        finish(record, complete, allow_partial, started)
        atomic_json(history, record)
        publish(stage, root, record, day)
        return record
    except BaseException as error:
        record.update(
            status="failed",
            ready=False,
            error=f"{type(error).__name__}: {error}",
            finished_at=iso(utcnow()),
            elapsed_s=round(time.monotonic() - started, 3),
        )
        atomic_json(history, record)
        raise
    finally:
        if stage.exists():
            shutil.rmtree(stage)
        if owned_spark and spark is not None:
            spark.stop()


def cli(doc, run, days_back, date_help, logger, argv=None):
    """Command line of both Silver jobs: --date and --allow-partial, then run."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=doc.splitlines()[0])
    default_day = (utcnow() - timedelta(days=days_back)).date()
    parser.add_argument("--date", type=Date.fromisoformat, default=default_day, help=date_help)
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="publish exploration output with ready=false when input is incomplete",
    )
    args = parser.parse_args(argv)
    landing = Path(os.environ.get("DAT535_LANDING_DIR", "landing"))
    output = Path(os.environ.get("DAT535_OUTPUT_DIR", "output"))
    try:
        run(landing, output, args.date, allow_partial=args.allow_partial)
    except (OSError, ValueError) as error:
        logger.error("%s", error)
        return 1
    return 0


def main(argv=None):
    date_help = "Norwegian operating day, YYYY-MM-DD (default: two days ago)"
    return cli(__doc__, run, 2, date_help, log, argv)


if __name__ == "__main__":
    sys.exit(main())
