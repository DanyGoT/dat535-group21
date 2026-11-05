#!/usr/bin/env python3
"""Entur Silver: archived plan/stops + Bronze ET, SX and vehicles -> reusable facts.

--date is a Norwegian OPERATING day, not a Bronze UTC partition. By default run
two days ago: the following UTC day must already have completed Bronze. Read
docs/pipeline/silver.md for contracts, readiness and rerun instructions.
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
from pathlib import Path

from bronze import DATASETS, HOURS, create_spark as bronze_spark, iso, run_id, utcnow
from silver_parse import decode_partition, schema
from silver_reference import (archived_file, nsr_partition, plan_members, plan_partition,
                              reference_schema)

log = logging.getLogger("silver")
JOURNEY_KEY = ["operating_date", "journey_id"]
CALL_KEY = JOURNEY_KEY + ["call_order"]
LEAD_MINUTES = [1, 5, 10, 20, 30]
SCHEMA_VERSION = 1
CONTRACT_ISSUES = frozenset({
    "bronze_corrupt", "bronze_incomplete", "bronze_wrong_endpoint", "invalid_vehicle_shape",
    "unknown_message_shape", "malformed_payload", "source_delivery_error", "graphql_error", "graphql_partial_error",
})


def functions():
    from pyspark.sql import functions as F
    return F


def empty_issues():
    return functions().array().cast("array<string>")


def issues(df, *rules):
    """Add named facts about invalid values. Keep the row and all its reasons."""
    F = functions()
    additions = [F.when(condition, F.lit(name)) for name, condition in rules]
    return df.withColumn("dq_issue", F.sort_array(F.array_distinct(F.concat(
        F.coalesce(F.col("dq_issue"), empty_issues()),
        F.filter(F.array(*additions).cast("array<string>"), lambda x: x.isNotNull())
    ))))


def assert_unique(df, key, name):
    F = functions()
    if df.filter(F.exists(F.array(*[F.col(k).isNull() for k in key]), lambda x: x)).limit(1).count():
        raise ValueError(f"{name}: null primary key {key}")
    if df.groupBy(*key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{name}: duplicate primary key {key}; refusing a multiplying join")


def current_reference(df):
    """An archive may repeat an operator at older native versions.

    Keep the highest numeric version, preserving ties for assert_unique to catch
    conflicting values at the SAME version. No lexical ordering of '9' vs '10'.
    """
    from pyspark.sql import Window
    F = functions()
    df = df.withColumn("_native_version", F.col("version").cast("decimal(38,0)"))
    return df.withColumn("_newest_version", F.max("_native_version").over(Window.partitionBy("id"))) \
        .filter(F.col("_native_version").eqNullSafe(F.col("_newest_version"))) \
        .drop("_native_version", "_newest_version")


def latest(df, key, actual_first=False):
    """Deterministic winner; producer time beats arrival order. No payload shuffle."""
    from pyspark.sql import Window
    F = functions()
    order = []
    if actual_first:
        order.append((F.col("actual_arrival_at").isNotNull() | F.col("actual_departure_at").isNotNull()).desc())
    if "recorded_at" in df.columns:
        order.append(F.col("recorded_at").desc_nulls_last())
    order += [F.col("received_at").desc_nulls_last(), F.col("payload_sha256").desc_nulls_last(),
              F.col("entity_index").desc_nulls_last()]
    return df.withColumn("_rank", F.row_number().over(Window.partitionBy(*key).orderBy(*order))) \
        .filter(F.col("_rank") == 1).drop("_rank")


def normalize_journeys(df, aliases):
    F = functions()
    df = df.withColumn("journey_id", F.coalesce("journey_id", "dated_journey_id"))
    mapped = aliases.select(F.col("dated_journey_id").alias("_alias"),
                            F.col("journey_id").alias("_mapped_journey"),
                            F.col("operating_date").alias("_mapped_date"))
    # Vehicles can carry both a normal ServiceJourney id and an explicit dated
    # reference. ET may carry the dated alias in journey_id itself instead.
    alias_ref = F.coalesce(F.col("dated_journey_id"), F.col("journey_id"))
    joined = df.join(mapped, alias_ref == mapped._alias, "left")
    joined = issues(joined, ("conflicting_operating_date", F.col("operating_date").isNotNull() &
                             F.col("_mapped_date").isNotNull() & (F.col("operating_date") != F.col("_mapped_date"))),
                    ("conflicting_journey_id", F.col("_mapped_journey").isNotNull() &
                     F.col("journey_id").isNotNull() & (F.col("journey_id") != F.col("_alias")) &
                     (F.col("journey_id") != F.col("_mapped_journey"))))
    return joined.withColumn("source_journey_id", F.col("journey_id")) \
        .withColumn("journey_id", F.coalesce("_mapped_journey", "journey_id")) \
        .withColumn("operating_date", F.coalesce("operating_date", "_mapped_date")) \
        .drop("_alias", "_mapped_journey", "_mapped_date")


def merge_plan(rt, plan, key):
    """Full outer join at the same grain: preserve the planned denominator."""
    F = functions()
    common = [c for c in ("line_id", "operator_id", "direction_id", "mode", "quay_id") if c in plan.columns]
    planned = plan.select(*key, *[F.col(c).alias("plan_" + c) for c in common],
                          *[F.col(c) for c in plan.columns if c not in key + common + ["dq_issue", "source_member"]],
                          F.col("dq_issue").alias("plan_dq_issue"),
                          F.col("source_member").alias("plan_source_member"), F.lit(True).alias("has_plan"))
    merged = rt.withColumn("has_realtime", F.lit(True)).join(planned, key, "full")
    for c in common:
        merged = merged.withColumn(c, F.coalesce(F.col(c), F.col("plan_" + c)))
    return merged.withColumn("dq_issue", F.array_distinct(F.concat(
        F.coalesce(F.col("dq_issue"), empty_issues()), F.coalesce(F.col("plan_dq_issue"), empty_issues())))) \
        .drop("plan_dq_issue") \
        .fillna(False, subset=["has_plan", "has_realtime"]) \
        .withColumn("realtime_status", F.when(~F.col("has_realtime"), "no_realtime")
                    .when(~F.col("has_plan"), "realtime_only").otherwise("matched"))


def conform(df, quays, lines):
    F = functions()
    if "quay_id" in df.columns:
        df = df.join(F.broadcast(quays.select(F.col("id").alias("quay_id"), F.lit(True).alias("_known_quay"))), "quay_id", "left")
        df = issues(df, ("unknown_quay", F.col("quay_id").isNotNull() & F.col("_known_quay").isNull())) \
            .drop("_known_quay")
    if "line_id" in df.columns:
        df = df.join(F.broadcast(lines.select(F.col("id").alias("line_id"), F.lit(True).alias("_known_line"))), "line_id", "left")
        df = issues(df, ("unknown_line", F.col("line_id").isNotNull() & F.col("_known_line").isNull())).drop("_known_line")
    return df


def build_journeys(versions, plan):
    columns = JOURNEY_KEY + ["line_id", "operator_id", "direction_id", "mode", "codespace", "vehicle_id",
                             "source_mode",
                             "journey_cancelled", "extra_journey", "monitored", "recorded_at", "received_at",
                             "payload_sha256", "record_sha256", "source_file", "bronze_run_id", "source_journey_id", "entity_index", "dq_issue"]
    rt = latest(versions.filter(functions().col("kind") == "journey").select(*columns), JOURNEY_KEY)
    return merge_plan(rt, plan, JOURNEY_KEY)


def build_calls(versions, plan, journeys):
    F = functions()
    calls = versions.filter(F.col("kind") == "call").drop("kind", "entity_json", "source_date", "journey_cancelled", "extra_journey")
    final = latest(calls, CALL_KEY, actual_first=True).withColumn("reported_quay_id", F.col("quay_id"))
    final = merge_plan(final, plan, CALL_KEY)
    # A cancellation-only journey message may have no calls. Its latest flag
    # still reaches every planned call, independently of the call-time winner.
    final = final.join(journeys.select(*JOURNEY_KEY, "journey_cancelled", "extra_journey",
                       F.col("has_realtime").alias("journey_has_realtime")), JOURNEY_KEY, "left")
    final = final.withColumn("quay_changed", F.col("has_plan") & F.col("has_realtime") &
                    (F.col("reported_quay_id") != F.col("plan_quay_id")))
    for event in ("arrival", "departure"):
        actual, aimed = F.col(f"actual_{event}_at"), F.col(f"aimed_{event}_at")
        # ET aimed remains distinct from the archived plan. Neither is an actual.
        final = final.withColumn(f"{event}_delay_s", actual.cast("double") - aimed.cast("double")) \
            .withColumn(f"actual_{event}_equals_aimed", actual == aimed)
    return issues(final, ("missing_quay", F.col("quay_id").isNull()),
                  ("missing_reported_quay", F.col("has_realtime") & F.col("reported_quay_id").isNull()),
                  ("nonpositive_call_order", F.col("call_order") <= 0),
                  ("departure_before_arrival", F.col("actual_departure_at") < F.col("actual_arrival_at")),
                  ("missing_aimed_time", F.col("has_realtime") & F.col("aimed_arrival_at").isNull() & F.col("aimed_departure_at").isNull()))


def asof_intervals(df, key, actual_column=None):
    """Build knowledge intervals without letting a late old replay undo new data."""
    from pyspark.sql import Window
    F = functions()
    df = df.filter(F.col("recorded_at").isNotNull() & F.col("received_at").isNotNull()) \
        .withColumn("available_at", F.greatest("recorded_at", "received_at"))
    priority = ([F.col(actual_column).isNotNull().alias("actual")] if actual_column else []) + [
        F.col("recorded_at"), F.col("received_at"), F.col("payload_sha256"), F.col("entity_index")]
    df = df.withColumn("_priority", F.struct(*priority))
    at_time = Window.partitionBy(*(key + ["available_at"])).orderBy(F.col("_priority").desc())
    df = df.withColumn("_same_time", F.row_number().over(at_time)).filter(F.col("_same_time") == 1).drop("_same_time")
    knowledge = Window.partitionBy(*key).orderBy("available_at")
    seen = knowledge.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn("_best_seen", F.max("_priority").over(seen)) \
        .filter(F.col("_priority") == F.col("_best_seen")).drop("_priority", "_best_seen")
    return df.withColumn("next_available_at", F.lead("available_at").over(knowledge))


def build_predictions(versions, final_calls, leads=LEAD_MINUTES):
    """As-of interval join, rather than expanding every version × every cutoff.

    available_at = max(producer, receive time). A version is visible only inside
    [available_at, next_available_at); this also prevents old estimates surviving
    a later actual or cancellation-only update.
    """
    F = functions()
    journey_states = asof_intervals(versions.filter(F.col("kind") == "journey"), JOURNEY_KEY).alias("j")
    parts = []
    for event in ("arrival", "departure"):
        v = versions.filter(F.col("kind") == "call").select(
            *CALL_KEY, "recorded_at", "received_at", "payload_sha256", "source_file", "bronze_run_id", "entity_index", "dq_issue",
            F.col(f"expected_{event}_at").alias("predicted_at"),
            F.col(f"actual_{event}_at").alias("known_actual_at"), "call_cancelled", "journey_cancelled")
        # One state at each knowledge time; retain states without a prediction so
        # they terminate the preceding state's interval.
        v = asof_intervals(v, CALL_KEY, actual_column="known_actual_at")
        target = final_calls.select(*CALL_KEY,
            F.coalesce(f"planned_{event}_at", f"aimed_{event}_at").alias("planned_target"),
            F.col(f"actual_{event}_at").alias("actual_target"))
        target = target.select(*CALL_KEY, F.explode(F.array(
            F.struct(F.lit("planned").alias("reference"), F.col("planned_target").alias("target_at")),
            F.struct(F.lit("actual").alias("reference"), F.col("actual_target").alias("target_at")))).alias("target")) \
            .select(*CALL_KEY, "target.*").filter(F.col("target_at").isNotNull()) \
            .withColumn("lead_minutes", F.explode(F.array(*[F.lit(n) for n in leads]))) \
            .withColumn("cutoff_at", F.timestamp_seconds(F.col("target_at").cast("double") - F.col("lead_minutes") * 60))
        target, v = target.alias("t"), v.alias("v")
        condition = (F.col("v.available_at") <= F.col("t.cutoff_at")) & \
            (F.col("v.next_available_at").isNull() | (F.col("t.cutoff_at") < F.col("v.next_available_at")))
        for key in CALL_KEY:
            condition = condition & (F.col("v." + key) == F.col("t." + key))
        # Keep planned targets even when no call update was known: a journey
        # cancellation can independently provide their state at the cutoff.
        joined = target.join(v, condition, "left").filter(F.col("v.known_actual_at").isNull())
        journey_condition = (F.col("j.available_at") <= F.col("t.cutoff_at")) & \
            (F.col("j.next_available_at").isNull() | (F.col("t.cutoff_at") < F.col("j.next_available_at")))
        for key in JOURNEY_KEY:
            journey_condition &= F.col("j." + key) == F.col("t." + key)
        joined = joined.join(journey_states, journey_condition, "left") \
            .filter(F.col("v.predicted_at").isNotNull() | (F.col("v.call_cancelled") == True) | (F.col("j.journey_cancelled") == True))
        parts.append(joined.select(*[F.col("t." + c).alias(c) for c in CALL_KEY],
                     F.lit(event).alias("event"), "t.reference", "t.target_at", "t.lead_minutes", "t.cutoff_at",
                     "v.predicted_at", F.greatest("v.available_at", "j.available_at").alias("available_at"),
                     F.coalesce("v.recorded_at", "j.recorded_at").alias("recorded_at"),
                     F.coalesce("v.received_at", "j.received_at").alias("received_at"),
                     "v.call_cancelled", "j.journey_cancelled",
                     F.col("j.recorded_at").alias("journey_recorded_at"), F.col("j.received_at").alias("journey_received_at"),
                     F.col("j.payload_sha256").alias("journey_payload_sha256"),
                     F.coalesce("v.payload_sha256", "j.payload_sha256").alias("payload_sha256"),
                     F.coalesce("v.source_file", "j.source_file").alias("source_file"),
                     F.coalesce("v.bronze_run_id", "j.bronze_run_id").alias("bronze_run_id"),
                     F.sort_array(F.array_distinct(F.concat(F.coalesce(F.col("v.dq_issue"), empty_issues()),
                                                          F.coalesce(F.col("j.dq_issue"), empty_issues())))).alias("dq_issue")))
    return parts[0].unionByName(parts[1])


def quarantine_rows(df, feed):
    F = functions()
    return df.select(F.lit(feed).alias("feed"), F.col("kind").alias("record_kind"), "source_date", "received_at",
                     "source_file", "payload_sha256", "record_sha256", "bronze_run_id", "dq_issue",
                     F.to_json(F.struct(*[F.col(c) for c in df.columns if c != "entity_json"])).alias("entity_json"))


def stage_table(df, stage, table, day, rid, key, partition="operating_date", metadata=None):
    F = functions()
    path = stage / table / f"{partition}={day}"
    if partition not in df.columns:
        df = df.withColumn(partition, F.lit(day))
    df = df.withColumn("_silver_run_id", F.lit(rid)).withColumn("_silver_schema_version", F.lit(SCHEMA_VERSION))
    for name, value in (metadata or {}).items():
        df = df.withColumn(name, F.lit(value).cast("timestamp") if name.endswith("_available_at") else F.lit(value))
    df.write.mode("overwrite").parquet(str(path))
    written = df.sparkSession.read.parquet(str(path))
    if key:
        assert_unique(written, key, table)
    stats = written.agg(F.count("*").alias("rows"),
                         F.sum((F.size("dq_issue") > 0).cast("long")).alias("flagged_rows"),
                         F.sum((F.col(partition).isNull() | (F.col(partition) != F.lit(day))).cast("long")).alias("wrong_partition_rows")).first().asDict()
    if stats.pop("wrong_partition_rows"):
        raise ValueError(f"{table}: rows do not belong to {partition}={day}")
    stats["flagged_rows"] = stats["flagged_rows"] or 0
    stats["issues"] = {r["issue"]: r["count"] for r in written.select(F.explode("dq_issue").alias("issue")).groupBy("issue").count().collect()}
    stats["parquet_bytes"] = sum(p.stat().st_size for p in path.glob("*.parquet"))
    log.info("%s: %s", table, stats)
    return written, stats


def atomic_json(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(record, indent=2, default=str) + "\n")
    os.replace(tmp, path)


def publish(stage, root, record, day):
    """Stage and check first; publish readiness last. Roll back a failed install.

    Runs/readers must use the documented VM lock. No landing/Bronze files are
    moved. Backups live inside this run's staging directory until commit.
    """
    marker = root / "_runs" / f"operating_date={day}" / "latest.json"
    previous = marker.read_text() if marker.exists() else None
    moved, installed = [], []
    atomic_json(marker, dict(record, status="publishing", ready=False))
    try:
        for table, info in record["tables"].items():
            relative = Path(table) / info["partition"]
            source, dest = stage / relative, root / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
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


def read_bronze(spark, output, feed, day, allow_partial, record):
    F = functions()
    days = [day + timedelta(days=n) for n in (-1, 0, 1)] if feed == "et" else [day]
    frames = []
    evidence = {}
    for source_day in days:
        folder = Path(output) / "bronze" / DATASETS[feed] / f"date={source_day}"
        if not (folder / "_SUCCESS").exists():
            evidence[str(source_day)] = {"missing_partition": True, "missing_hours": HOURS}
            continue
        frame = spark.read.parquet(str(folder))
        audit = frame.groupBy("_bronze_status", "_bronze_run_id").agg(
            F.count("*").alias("rows"), F.collect_set("hour").alias("hours")).collect()
        hours = {h for r in audit for h in r["hours"]}
        missing = sorted(set(HOURS) - hours)
        statuses = {}
        for r in audit:
            statuses[r["_bronze_status"]] = statuses.get(r["_bronze_status"], 0) + r["rows"]
        evidence[str(source_day)] = {"missing_hours": missing, "rows": sum(r["rows"] for r in audit),
                                     "bronze_status": statuses, "bronze_run_ids": sorted({r["_bronze_run_id"] for r in audit}),
                                     "parquet_bytes": sum(p.stat().st_size for p in folder.glob("*.parquet"))}
        frames.append(frame.withColumn("source_date", F.lit(str(source_day))))
    record["inputs"][feed] = evidence
    complete = all(not entry["missing_hours"] for entry in evidence.values())
    if not complete and not allow_partial:
        raise ValueError(f"incomplete {feed} Bronze coverage: {evidence}; use --allow-partial only for exploration")
    if not frames:
        raise FileNotFoundError(f"no completed Bronze input for {feed} near {day}")
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    raw_line = F.col("_bronze_raw_line") if "_bronze_raw_line" in result.columns else F.lit(None).cast("string")
    result = result.withColumn("record_sha256", F.coalesce("_bronze_payload_sha256", F.sha2(raw_line, 256),
        F.sha2(F.to_json(F.struct("received_at", "_bronze_status", "_bronze_source_file")), 256)))
    return result.select("received_at", "payload", "_bronze_source_file", "_bronze_payload_sha256",
                         "record_sha256", "_bronze_run_id", "_bronze_status", "source_date"), complete


def run(landing, output_dir, day, spark=None, allow_partial=False, leads=LEAD_MINUTES):
    F = functions()
    root = Path(output_dir) / "silver"
    if root.resolve().is_relative_to(Path(landing).resolve()):
        raise ValueError("Silver output must be outside the immutable landing directory")
    rid = run_id()
    started = time.monotonic()
    stage = root / "_staging" / (rid + "-" + uuid.uuid4().hex[:8])
    record = dict(run_id=rid, env=os.environ.get("DAT535_ENV", "local"), schema_version=SCHEMA_VERSION,
                  operating_date=str(day), started_at=iso(utcnow()), status="running", ready=False,
                  inputs={}, tables={}, lead_minutes=list(leads))
    history = root / "_runs" / f"operating_date={day}" / f"{rid}.json"
    owned_spark = spark is None
    try:
        snapshots = {dataset: archived_file(landing, dataset, day) for dataset in ("netex", "nsr")}
        record["references"] = {k: {"snapshot_date": str(d), "file": str(p), "bytes": p.stat().st_size}
                                for k, (d, p) in snapshots.items()}
        for dataset, (_, path) in snapshots.items():
            meta = path.with_name(path.name + ".meta.json")
            if meta.exists():
                archived = json.loads(meta.read_text())
                if archived["bytes"] != path.stat().st_size:
                    raise ValueError(f"archive size differs from collector manifest: {path}")
                record["references"][dataset]["sha256"] = archived["sha256"]
                record["references"][dataset]["available_at"] = archived.get("finished_at")
            else:
                with path.open("rb") as source:
                    record["references"][dataset]["sha256"] = hashlib.file_digest(source, "sha256").hexdigest()
        if spark is None:
            scratch = Path(os.environ.get("DAT535_SPARK_LOCAL_DIR", str(Path(output_dir).parent / "spark-scratch")))
            scratch.mkdir(parents=True, exist_ok=True)
            spark = bronze_spark("DAT535-Silver-Entur", settings={
                "spark.master": os.environ.get("SPARK_MASTER", "local[4]"),
                "spark.local.dir": str(scratch),
                "spark.sql.shuffle.partitions": os.environ.get("DAT535_SHUFFLE_PARTITIONS", "32")})
        for module in ("silver.py", "silver_parse.py", "silver_reference.py"):
            spark.sparkContext.addPyFile(str(Path(__file__).with_name(module)))
        stage.mkdir(parents=True)
        inputs, complete = {}, True
        for feed in DATASETS:
            inputs[feed], covered = read_bronze(spark, output_dir, feed, day, allow_partial, record)
            complete &= covered
        netex_day, netex_path = snapshots["netex"]
        nsr_day, nsr_path = snapshots["nsr"]
        members = plan_members(netex_path)
        # Shared members also provide operators. They emit no ServiceJourneys.
        with zipfile.ZipFile(netex_path) as z:
            shared = sorted(n for n in z.namelist() if n.startswith("_") and n.endswith("shared_data.xml"))
        members += [(s, None) for s in shared]
        plan_rdd = spark.sparkContext.parallelize(members, min(len(members), spark.sparkContext.defaultParallelism * 4)) \
            .mapPartitions(lambda it: plan_partition(it, str(netex_path), day))
        plan_path = stage / "_work" / "plan"
        spark.createDataFrame(plan_rdd, reference_schema()).write.parquet(str(plan_path))
        planned = spark.read.parquet(str(plan_path))
        if not planned.filter(F.col("kind") == "journey").limit(1).count():
            raise ValueError(f"archived timetable contains no supported journeys for {day}")
        with zipfile.ZipFile(nsr_path) as z:
            nsr_members = sorted(n for n in z.namelist() if n.endswith(".xml"))
        nsr_rdd = spark.sparkContext.parallelize(nsr_members, min(len(nsr_members), spark.sparkContext.defaultParallelism)) \
            .mapPartitions(lambda it: nsr_partition(it, str(nsr_path)))
        nsr_work = stage / "_work" / "nsr"
        spark.createDataFrame(nsr_rdd, reference_schema()).write.parquet(str(nsr_work))
        stops = spark.read.parquet(str(nsr_work))

        def save(df, table, key, partition="operating_date", metadata=None):
            written, stats = stage_table(df, stage, table, day, rid, key, partition, metadata)
            stats["partition"] = f"{partition}={day}"
            record["tables"][table] = stats
            return written

        dimensions = {}
        for table, source, columns in (
            ("quay", stops, ["id", "version", "name", "stop_place_id", "latitude", "longitude", "source_member", "dq_issue"]),
            ("stop_place", stops, ["id", "version", "name", "parent_stop_place_id", "latitude", "longitude", "source_member", "dq_issue"]),
            ("line", planned, ["id", "version", "name", "operator_id", "mode", "public_code", "is_flexible", "source_member", "dq_issue"]),
            ("operator", planned, ["id", "version", "name", "source_member", "dq_issue"])):
            dim = source.filter(F.col("kind") == table).select(*columns)
            # Identical references can be repeated in different line members. An
            # actual conflicting value fails uniqueness instead of picking one.
            dim = current_reference(dim.drop("source_member").dropDuplicates())
            if table in {"quay", "stop_place"}:
                dim = issues(dim, ("missing_coordinates", F.col("latitude").isNull() | F.col("longitude").isNull()),
                             ("invalid_coordinates", ~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180)))
            dimensions[table] = save(dim, table, ["id"], metadata={"snapshot_date": nsr_day if table in {"quay", "stop_place"} else netex_day})
            if table in {"quay", "stop_place", "line"} and record["tables"][table]["rows"] == 0:
                raise ValueError(f"reference contract: no {table} entities decoded")
        aliases = planned.filter(F.col("kind") == "journey").select(*JOURNEY_KEY,
                         F.explode("dated_journey_ids").alias("dated_journey_id")).dropDuplicates()
        assert_unique(aliases, ["dated_journey_id"], "dated journey mapping")
        aliases = save(aliases.withColumn("dq_issue", empty_issues()), "dated_journey", ["dated_journey_id"],
                       metadata={"snapshot_date": netex_day})
        plan_journey = planned.filter(F.col("kind") == "journey").select(
            *JOURNEY_KEY, "line_id", "operator_id", "direction_type", "mode", "journey_pattern_id", "service_alteration",
            "dated_journey_ids", "is_flexible", "source_member", "dq_issue")
        plan_call = planned.filter(F.col("kind") == "call").select(
            *CALL_KEY, "line_id", "operator_id", "direction_type", "mode", "quay_id", "planned_arrival_at", "planned_departure_at",
            "earliest_departure_at", "latest_arrival_at", "is_flexible", "for_boarding", "for_alighting", "source_member", "dq_issue")
        assert_unique(plan_journey, JOURNEY_KEY, "plan journeys")
        assert_unique(plan_call, CALL_KEY, "plan calls")
        quarantine = []
        decoded = {}
        # Check the small feeds before spending hours decoding nationwide ET.
        for feed in ("sx", "vehicles", "et"):
            source = inputs[feed]
            work = stage / "_work" / feed
            parsed = spark.createDataFrame(source.rdd.mapPartitions(
                lambda it, feed=feed: decode_partition(it, feed, target_day=day if feed == "et" else None)), schema(feed))
            parsed.write.parquet(str(work))
            parsed = spark.read.parquet(str(work))
            if feed == "et":
                record["inputs"][feed]["decode_scope"] = "target day, unresolved dates and unusable identities; controls/quarantine retained"
            record["inputs"][feed]["decoded"] = {r["kind"]: r["count"] for r in parsed.groupBy("kind").count().collect()}
            bad = parsed.filter(F.col("kind") == "quarantine")
            quarantine.append(quarantine_rows(bad, feed))
            parsed = parsed.filter(~F.col("kind").isin("quarantine", "control"))
            if feed in {"et", "vehicles"}:
                parsed = normalize_journeys(parsed, aliases)
                parsed = issues(parsed, ("missing_operating_date", F.col("operating_date").isNull()),
                                ("missing_journey_id", F.col("journey_id").isNull()))
                if feed == "et":
                    valid = F.col("operating_date").isNotNull() & F.col("journey_id").isNotNull()
                    parsed = issues(parsed, ("missing_call_order", (F.col("kind") == "call") & F.col("call_order").isNull()),
                                    ("missing_recorded_at", F.col("recorded_at").isNull()))
                    valid &= (F.col("kind") != "call") | F.col("call_order").isNotNull()
                else:
                    parsed = issues(parsed, ("missing_vehicle_id", F.col("vehicle_id").isNull()),
                                    ("missing_codespace", F.col("codespace").isNull()),
                                    ("missing_last_updated_at", F.col("last_updated_at").isNull()))
                    valid = F.col("vehicle_id").isNotNull() & F.col("codespace").isNotNull() & F.col("last_updated_at").isNotNull()
                quarantine.append(quarantine_rows(parsed.filter(~valid), feed))
                parsed = parsed.filter(valid)
                if feed == "et":
                    parsed = parsed.filter(F.col("operating_date") == F.lit(day))
            else:
                parsed = issues(parsed, ("missing_situation_id", F.col("situation_id").isNull()),
                                ("missing_participant_id", F.col("participant_id").isNull()))
                valid = F.col("situation_id").isNotNull() & F.col("participant_id").isNotNull()
                quarantine.append(quarantine_rows(parsed.filter(~valid), feed))
                parsed = parsed.filter(valid)
            scoped_work = stage / "_work" / (feed + "-scoped")
            parsed.write.parquet(str(scoped_work))
            decoded[feed] = spark.read.parquet(str(scoped_work))
            record["inputs"][feed]["retained_versions"] = {r["kind"]: r["count"] for r in
                decoded[feed].groupBy("kind").count().collect()}
        metadata = {"netex_snapshot_date": netex_day, "nsr_snapshot_date": nsr_day,
                    "netex_available_at": record["references"]["netex"].get("available_at"),
                    "nsr_available_at": record["references"]["nsr"].get("available_at")}
        journeys = conform(build_journeys(decoded["et"], plan_journey), dimensions["quay"], dimensions["line"])
        journeys = save(journeys, "journey", JOURNEY_KEY, metadata=metadata)
        calls = conform(build_calls(decoded["et"], plan_call, journeys), dimensions["quay"], dimensions["line"])
        calls = save(calls, "stop_call", CALL_KEY, metadata=metadata)
        record["call_coverage"] = {r["realtime_status"]: r["count"] for r in calls.groupBy("realtime_status").count().collect()}
        record["call_reconciliation"] = {
            "input_versions": record["inputs"]["et"]["retained_versions"].get("call", 0),
            "planned_calls": record["call_coverage"].get("matched", 0) + record["call_coverage"].get("no_realtime", 0),
            "reported_calls": record["call_coverage"].get("matched", 0) + record["call_coverage"].get("realtime_only", 0)}
        record["call_reconciliation"]["duplicate_versions_removed"] = record["call_reconciliation"]["input_versions"] - record["call_reconciliation"]["reported_calls"]
        record["actual_equals_aimed_by_operator"] = [r.asDict() for r in calls.groupBy("operator_id").agg(
            F.count("actual_departure_at").alias("actual_departures"),
            F.sum(F.coalesce(F.col("actual_departure_equals_aimed"), F.lit(False)).cast("long")).alias("actual_equals_aimed")).collect()]
        predictions = build_predictions(decoded["et"], calls, leads)
        save(predictions, "call_prediction", CALL_KEY + ["event", "reference", "lead_minutes"], metadata=metadata)
        vehicles = latest(decoded["vehicles"], ["codespace", "vehicle_id", "last_updated_at"])
        vehicles = issues(vehicles, ("missing_coordinates", F.col("latitude").isNull() | F.col("longitude").isNull()),
                          ("invalid_coordinates", ~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180)),
                          ("invalid_bearing", ~F.col("bearing").between(0, 360)),
                          ("negative_speed", F.col("speed") < 0),
                          ("invalid_progress", ~F.col("progress_pct").between(0, 100)))
        vehicles = conform(vehicles, dimensions["quay"], dimensions["line"])
        save(vehicles.drop("kind", "entity_json", "source_date"), "vehicle_position",
             ["codespace", "vehicle_id", "last_updated_at"], partition="observation_date", metadata=metadata)
        from pyspark.sql import Window
        sx = decoded["sx"].withColumn("first_received_at", F.min("received_at").over(
            Window.partitionBy("participant_id", "situation_id", "version_key")))
        sx = latest(sx, ["participant_id", "situation_id", "version_key"])
        sx = issues(sx, ("invalid_validity_period", F.exists("validity_periods", lambda p: p.end_at < p.start_at)))
        save(sx.drop("kind", "entity_json", "source_date"), "sx_situation", ["participant_id", "situation_id", "version_key"], partition="observation_date")
        rejected = quarantine[0]
        for frame in quarantine[1:]:
            rejected = rejected.unionByName(frame)
        save(rejected, "quarantine", None)
        contract_errors = record["tables"]["quarantine"]["issues"]
        record["contract_errors"] = {k: n for k, n in contract_errors.items() if k in CONTRACT_ISSUES}
        record["finished_at"] = iso(utcnow())
        record["elapsed_s"] = round(time.monotonic() - started, 3)
        record["status"] = "complete" if complete and not record["contract_errors"] else "partial"
        record["ready"] = record["status"] == "complete"
        if record["contract_errors"] and not allow_partial:
            raise ValueError(f"source contract errors: {record['contract_errors']}")
        atomic_json(history, record)
        publish(stage, root, record, day)
        return record
    except BaseException as error:
        record.update(status="failed", ready=False, error=f"{type(error).__name__}: {error}",
                      finished_at=iso(utcnow()), elapsed_s=round(time.monotonic() - started, 3))
        atomic_json(history, record)
        raise
    finally:
        if stage.exists():
            shutil.rmtree(stage)
        if owned_spark and spark is not None:
            spark.stop()


def main(argv=None):
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", type=Date.fromisoformat, default=(utcnow() - timedelta(days=2)).date(),
                        help="Norwegian operating day, YYYY-MM-DD (default: two days ago)")
    parser.add_argument("--allow-partial", action="store_true", help="publish exploration output with ready=false when input is incomplete")
    args = parser.parse_args(argv)
    try:
        run(Path(os.environ.get("DAT535_LANDING_DIR", "landing")), Path(os.environ.get("DAT535_OUTPUT_DIR", "output")),
            args.date, allow_partial=args.allow_partial)
    except (OSError, ValueError) as error:
        log.error("%s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
