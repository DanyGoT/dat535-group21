#!/usr/bin/env python3
"""DATEX Silver: one UTC day of polled snapshots -> road situations, weather and travel times.

--date is the UTC day of the polls: DATEX has no operating day. Run after that day's Bronze.
Contracts, readiness and what the gaps in the data mean: docs/pipeline/silver.md.

Same machinery as Entur Silver: stage, check keys, count issues, publish, marker last. The
marker is silver/_runs/datex/observation_date=D/latest.json.
"""

import argparse
import logging
import os
import shutil
import sys
import time
import uuid
from datetime import date as Date, timedelta
from functools import partial
from pathlib import Path

from bronze import HOURS, create_spark as bronze_spark, iso, run_id, utcnow
from silver import CONTRACT_ISSUES, atomic_json, functions, issues, publish, stage_table
from silver_datex_parse import decode_partition, schema, section_points, situation_points
from silver_parse import arrow_decoder

log = logging.getLogger("silver_datex")
SCHEMA_VERSION = 1
SITUATION_KEY = ["record_id", "version", "version_at"]
STREAMS = ("situation", "traveltime", "weather")
REFERENCES = ("traveltime-locations", "weather-sites")
# The kind of row each feed publishes, and its key. stage_table refuses a null key.
KEYS = {"situation": ("record", SITUATION_KEY), "weather": ("value", ["site_id", "measurement_at"]),
        "traveltime": ("value", ["section_id", "period_start_at"]),
        "weather-sites": ("site", ["id"]), "traveltime-locations": ("section", ["id"])}
# Every situation row is a ~27 MB snapshot: one per Arrow record and per Parquet read batch.
# Four per read batch filled a 3 GB heap on 2026-11-03 (docs/report/problems.md, 2026-11-07).
ONE_SNAPSHOT_AT_A_TIME = {"spark.sql.execution.arrow.maxRecordsPerBatch": "1",
                          "spark.sql.parquet.columnarReaderBatchSize": "1"}


def read_bronze(spark, output, feed, day, allow_partial, record):
    """The day's polls of one feed, with their audit. A location table is polled four times a
    day, so only the streaming feeds must cover every hour."""
    F = functions()
    folder = Path(output) / "bronze" / f"datex-{feed}" / f"date={day}"
    if not (folder / "_SUCCESS").exists():
        evidence = dict(missing_partition=True)
        record["inputs"][feed] = evidence
        if not allow_partial:
            raise ValueError(f"no Bronze for datex-{feed} on {day}; use --allow-partial only for exploration")
        return None, False
    frame = spark.read.parquet(str(folder))
    audit = frame.groupBy("_bronze_status", "_bronze_run_id").agg(F.count("*").alias("rows"), F.collect_set("hour").alias("hours")).collect()
    statuses = {}
    for r in audit:
        statuses[r["_bronze_status"]] = statuses.get(r["_bronze_status"], 0) + r["rows"]
    hours = {h for r in audit for h in r["hours"]}
    missing = sorted(set(HOURS) - hours) if feed in STREAMS else []
    ok = statuses.get("ok", 0) + statuses.get("not_modified", 0)
    record["inputs"][feed] = dict(missing_hours=missing, rows=sum(statuses.values()), bronze_status=statuses,
                                  bronze_run_ids=sorted({r["_bronze_run_id"] for r in audit}))
    complete = not missing and ok > 0
    if not complete and not allow_partial:
        raise ValueError(f"incomplete datex-{feed} Bronze: {record['inputs'][feed]}; use --allow-partial only for exploration")
    return frame.withColumn("source_date", F.lit(str(day))).select(
        "received_at", "payload", "_bronze_source_file", "_bronze_payload_sha256", "_bronze_run_id", "_bronze_status", "source_date"), complete


def decode(spark, source, feed, work):
    previous = {k: spark.conf.get(k) for k in ONE_SNAPSHOT_AT_A_TIME}
    for key, value in ONE_SNAPSHOT_AT_A_TIME.items():
        spark.conf.set(key, value)
    try:
        spec = schema(feed)
        source.mapInArrow(arrow_decoder(partial(decode_partition, feed=feed), spec), spec).write.parquet(str(work))
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)
    return spark.read.parquet(str(work))


def split_unkeyed(frame, feed):
    """(rows with their whole key, quarantine rows). A row without its key cannot be published, so
    it is quarantined, never silently dropped. A situation's sightings without a key are dropped:
    the record they would join to is quarantined."""
    F = functions()
    kind, key = KEYS[feed]
    unkeyed = F.exists(F.array(*[F.col(k).isNull() for k in key]), lambda x: x)
    bad = issues(frame.filter((F.col("kind") == kind) & unkeyed), *[(f"missing_{k}", F.col(k).isNull()) for k in key]) \
        .withColumn("entity_json", F.to_json(F.struct(*[c for c in frame.columns if c != "entity_json"])))
    bad = frame.filter(F.col("kind") == "quarantine").unionByName(bad)
    return frame.filter((F.col("kind") != "quarantine") & ~unkeyed), bad.select(
        F.lit(feed).alias("feed"), "received_at", "source_file", "payload_sha256", "bronze_run_id", "dq_issue", "entity_json")


def first_and_last(df, key):
    """One row per key with its earliest copy's values, plus when it was first and last seen."""
    F = functions()
    values = [c for c in df.columns if c not in key]
    return df.groupBy(*key).agg(
        F.min_by(F.struct(*values), F.struct("received_at", "payload_sha256")).alias("_earliest"),
        F.min("received_at").alias("first_received_at"), F.max("received_at").alias("last_received_at"),
        F.count("*").alias("copies")) \
        .select(*key, "_earliest.*", "first_received_at", "last_received_at", "copies")


def build_situations(parsed):
    F = functions()
    key = SITUATION_KEY
    sightings = parsed.filter(F.col("kind") == "sighting").groupBy(*key).agg(
        F.min("received_at").alias("first_seen_at"), F.max("received_at").alias("last_seen_at"), F.count("*").alias("snapshots_seen"))
    records = first_and_last(parsed.filter(F.col("kind") == "record"), key).drop("first_received_at", "last_received_at", "copies")
    situations = records.join(sightings, key).drop("kind", "entity_json")
    return issues(situations,
                  ("missing_start", F.col("start_at").isNull()),
                  ("end_before_start", F.col("end_at") < F.col("start_at")),
                  ("version_before_created", F.col("version_at") < F.col("created_at")),
                  ("missing_location", F.size("locations") == 0),
                  ("coordinates_outside_norway", F.exists("locations", lambda l: ~l.latitude.between(57, 72) | ~l.longitude.between(3, 33))))


def build_intervals(situations):
    F = functions()
    columns = SITUATION_KEY + ["record_type", "record_subtypes", "severity", "dq_issue"]
    return situations.select(*columns, F.explode("active_intervals").alias("interval")) \
        .select(*columns, "interval.start_at", "interval.end_at")


def build_weather(parsed):
    F = functions()
    weather = first_and_last(parsed.filter(F.col("kind") == "value"), ["site_id", "measurement_at"]).drop("kind", "entity_json", "copies")
    air, surface, wind = F.col("air_temperature_c"), F.col("road_surface_temperature_c"), F.col("wind_speed")
    # Measured on 2026-11-05: frozen stations (up to 1.6 years behind), air -39.9 at sites whose
    # road surface was above zero, humidity 0, dew point -81.9, a constant 80.3 mm/h. Flagged, kept.
    return issues(weather,
                  ("stale_measurement", F.col("first_received_at").cast("long") - F.col("measurement_at").cast("long") > 3600),
                  ("suspect_air_temperature", ~air.between(-50, 40) | (F.abs(air - surface) > 30)),
                  ("suspect_road_temperature", ~surface.between(-50, 70)),
                  ("suspect_humidity", (F.col("relative_humidity_pct") <= 0) | (F.col("relative_humidity_pct") > 105)),
                  ("suspect_dew_point", F.col("dew_point_c") < -60),
                  ("suspect_precipitation", F.col("precipitation_mm_h") > 50),
                  ("suspect_wind", (wind > 60) | (F.col("wind_gust") > 80)),
                  ("suspect_friction", ~F.col("friction").between(0, 1)))


def build_travel_times(parsed):
    F = functions()
    times = first_and_last(parsed.filter(F.col("kind") == "value"), ["section_id", "period_start_at"]).drop("kind", "entity_json", "copies")
    times = times.withColumn("has_measurement", F.col("travel_time_s").isNotNull()) \
        .withColumn("congestion_ratio", F.col("travel_time_s") / F.col("free_flow_s"))
    return issues(times,
                  ("nonpositive_travel_time", F.col("travel_time_s") <= 0),
                  ("nonpositive_free_flow", F.col("free_flow_s") <= 0),
                  ("measurement_without_status", F.col("has_measurement") & (F.col("traffic_status") == "unknown")))


def latest_reference(parsed, kind):
    """The day's last snapshot of a location table: its ids are the reference for that day."""
    F = functions()
    rows = parsed.filter(F.col("kind") == kind).drop("kind", "entity_json")
    values = [c for c in rows.columns if c != "id"]
    return rows.groupBy("id").agg(F.max_by(F.struct(*values), F.struct("received_at", "payload_sha256")).alias("_latest")) \
        .select("id", "_latest.*").select(*rows.columns)


def build_sites(parsed, weather):
    """Reference plus how the station behaved that day: which ones Gold may map a quay to."""
    F = functions()
    sites = latest_reference(parsed, "site")
    # A non-zero value that never moves all day is a stuck sensor. Zero precipitation is a dry
    # day and 20,000 m is the visibility sensors' upper limit, so neither counts.
    checked = [c for c in ("air_temperature_c", "road_surface_temperature_c", "relative_humidity_pct", "precipitation_mm_h", "wind_speed") if c in weather.columns]
    current = weather.filter(~F.array_contains("dq_issue", "stale_measurement"))
    stats = current.groupBy("site_id").agg(
        F.count("*").alias("measurements"), F.max("measurement_at").alias("last_measurement_at"),
        *[(F.count_distinct(c) == 1).alias("_one_" + c) for c in checked], *[F.max(c).alias("_max_" + c) for c in checked])
    stuck = F.filter(F.array(*[F.when(F.col("_one_" + c) & (F.col("measurements") >= 20) & (F.col("_max_" + c) != 0), F.lit(c)) for c in checked]),
                     lambda x: x.isNotNull())
    stats = stats.withColumn("stuck_quantities", stuck).select("site_id", "measurements", "last_measurement_at", "stuck_quantities")
    frozen = weather.filter(F.array_contains("dq_issue", "stale_measurement")).groupBy("site_id").agg(F.max("measurement_at").alias("_frozen_at"))
    out = sites.join(stats, sites.id == stats.site_id, "left").drop("site_id").join(frozen, sites.id == frozen.site_id, "left").drop("site_id") \
        .withColumn("last_measurement_at", F.coalesce("last_measurement_at", "_frozen_at")).drop("_frozen_at") \
        .fillna(0, subset=["measurements"]).withColumn("usable", F.col("measurements") >= 72)
    return issues(out, ("no_current_measurement", F.col("measurements") == 0),
                  ("missing_coordinates", F.col("latitude").isNull() | F.col("longitude").isNull()))


def build_sections(parsed, times):
    F = functions()
    sections = latest_reference(parsed, "section")
    stats = times.groupBy("section_id").agg(F.count("*").alias("periods"),
                                            F.sum(F.col("has_measurement").cast("long")).alias("measured_periods"))
    out = sections.join(stats, sections.id == stats.section_id, "left").drop("section_id").fillna(0, subset=["periods", "measured_periods"])
    return issues(out, ("missing_geometry", F.col("path").isNull() | (F.size("path") < 2)),
                  ("never_measured", F.col("measured_periods") == 0))


def newest_quays(spark, root, day):
    """NSR changes slowly and Entur Silver for D runs a day after this job: the newest quay
    snapshot on or before D. None before the first Entur Silver run."""
    F = functions()
    folders = sorted(p for p in (root / "quay").glob("operating_date=*") if p.name.removeprefix("operating_date=") <= str(day) and (p / "_SUCCESS").exists())
    if not folders:
        return None, None
    quays = spark.read.parquet(str(folders[-1])).filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    return quays.select(F.col("id").alias("quay_id"), F.col("latitude").alias("quay_latitude"), F.col("longitude").alias("quay_longitude")), folders[-1].name.removeprefix("operating_date=")


def haversine_m(lat1, lon1, lat2, lon2):
    F = functions()
    x = F.sin(F.radians(lat2 - lat1) / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(F.radians(lon2 - lon1) / 2) ** 2
    return F.lit(2 * 6371000.0) * F.asin(F.sqrt(x))


def near(quays, points, key, radius_m):
    """One row per (quay, key) within radius_m, with its smallest distance. Grid cells of 0.01 deg
    latitude (1.1 km) and 0.02 deg longitude (0.76-1.2 km across Norway): a point within 500 m of
    a quay is always in the quay's cell or a neighbour, so this compares a few points per quay
    instead of 100,000 quays x 100,000 points."""
    F = functions()
    around = F.array(F.lit(-1), F.lit(0), F.lit(1))
    q = quays.withColumn("_d_lat", F.explode(around)).withColumn("_d_lon", F.explode(around)) \
        .withColumn("_cell_lat", F.floor(F.col("quay_latitude") / 0.01) + F.col("_d_lat")) \
        .withColumn("_cell_lon", F.floor(F.col("quay_longitude") / 0.02) + F.col("_d_lon")).drop("_d_lat", "_d_lon")
    p = points.withColumn("_cell_lat", F.floor(F.col("latitude") / 0.01)).withColumn("_cell_lon", F.floor(F.col("longitude") / 0.02))
    joined = q.join(p, ["_cell_lat", "_cell_lon"]).withColumn(
        "distance_m", haversine_m(F.col("quay_latitude"), F.col("quay_longitude"), F.col("latitude"), F.col("longitude")))
    return joined.filter(F.col("distance_m") <= radius_m).groupBy("quay_id", *key).agg(F.round(F.min("distance_m"), 1).alias("distance_m"))


def build_quay_weather_site(quays, sites):
    """Each quay's nearest station that worked that day. Road stations are inland: the distance
    is the point (ferry quays are a median 14 km away, docs/datex/content.md), so Gold decides how
    far is too far. One row per quay; the join to facts cannot multiply them."""
    F = functions()
    usable = sites.filter(F.col("usable") & F.col("latitude").isNotNull()).select(
        F.col("id").alias("site_id"), F.col("latitude").alias("site_latitude"), F.col("longitude").alias("site_longitude"))
    pairs = quays.crossJoin(F.broadcast(usable)).withColumn(
        "distance_m", haversine_m(F.col("quay_latitude"), F.col("quay_longitude"), F.col("site_latitude"), F.col("site_longitude")))
    return pairs.groupBy("quay_id").agg(F.min_by(F.struct("site_id", "distance_m"), "distance_m").alias("nearest")) \
        .select("quay_id", "nearest.site_id", F.round("nearest.distance_m", 1).alias("distance_m")) \
        .withColumn("dq_issue", F.when(F.col("distance_m") > 30000, F.array(F.lit("nearest_site_beyond_30km"))).otherwise(F.array().cast("array<string>")))


def run(landing, output_dir, day, spark=None, allow_partial=False):
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
    F = functions()
    root = Path(output_dir) / "silver"
    rid = run_id()
    started = time.monotonic()
    stage = root / "_staging" / (rid + "-datex-" + uuid.uuid4().hex[:8])
    runs = root / "_runs" / "datex" / f"observation_date={day}"
    record = dict(run_id=rid, source="datex", env=os.environ.get("DAT535_ENV", "local"), schema_version=SCHEMA_VERSION,
                  observation_date=str(day), started_at=iso(utcnow()), status="running", ready=False, inputs={}, tables={})
    owned_spark = spark is None
    try:
        if spark is None:
            scratch = Path(os.environ.get("DAT535_SPARK_LOCAL_DIR", str(Path(output_dir).parent / "spark-scratch")))
            scratch.mkdir(parents=True, exist_ok=True)
            spark = bronze_spark("DAT535-Silver-DATEX", settings={
                "spark.master": os.environ.get("SPARK_MASTER", "local[3]"), "spark.local.dir": str(scratch),
                "spark.sql.shuffle.partitions": os.environ.get("DAT535_SHUFFLE_PARTITIONS", "4")})
        for module in ("silver.py", "silver_parse.py", "silver_reference.py", "silver_datex_parse.py", "bronze.py"):
            spark.sparkContext.addPyFile(str(Path(__file__).with_name(module)))
        stage.mkdir(parents=True)
        parsed, complete = {}, True
        for feed in STREAMS + REFERENCES:
            source, covered = read_bronze(spark, output_dir, feed, day, allow_partial, record)
            complete &= covered
            if source is None:
                continue
            parsed[feed] = decode(spark, source, feed, stage / "_work" / feed)
            record["inputs"][feed]["decoded"] = {r["kind"]: r["count"] for r in parsed[feed].groupBy("kind").count().collect()}

        def save(df, table, key):
            written, stats = stage_table(df, stage, table, day, rid, key, "observation_date")
            stats["partition"] = f"observation_date={day}"
            record["tables"][table] = stats
            return written

        rejected = None
        for feed in list(parsed):
            parsed[feed], bad = split_unkeyed(parsed[feed], feed)
            rejected = bad if rejected is None else rejected.unionByName(bad)
        if "situation" in parsed:
            situations = save(build_situations(parsed["situation"]), "road_situation", SITUATION_KEY)
            save(build_intervals(situations), "road_situation_interval", SITUATION_KEY + ["start_at"])
        if "weather" in parsed:
            weather = save(build_weather(parsed["weather"]), "road_weather", ["site_id", "measurement_at"])
            if "weather-sites" in parsed:
                save(build_sites(parsed["weather-sites"], weather), "weather_site", ["id"])
        if "traveltime" in parsed:
            times = save(build_travel_times(parsed["traveltime"]), "road_travel_time", ["section_id", "period_start_at"])
            if "traveltime-locations" in parsed:
                save(build_sections(parsed["traveltime-locations"], times), "road_section", ["id"])
        if rejected is not None:
            save(rejected, "datex_quarantine", None)
        quays, quay_snapshot = newest_quays(spark, root, day)
        record["quay_snapshot_date"] = quay_snapshot
        if quays is None:
            log.warning("no Entur Silver quay snapshot on or before %s: spatial links skipped", day)
        elif {"road_situation", "weather_site", "road_section"} <= set(record["tables"]):
            quays = quays.cache()
            spec = StructType([StructField(n, t) for n, t in (("record_id", StringType()), ("version", StringType()), ("version_at", TimestampType()),
                                                              ("latitude", DoubleType()), ("longitude", DoubleType()))])
            points = situations.select(*SITUATION_KEY, "locations").mapInArrow(arrow_decoder(situation_points, spec), spec)
            links = near(quays, points, SITUATION_KEY, 500).withColumn("dq_issue", F.array().cast("array<string>"))
            save(links, "quay_road_situation", ["quay_id"] + SITUATION_KEY)
            sites = spark.read.parquet(str(stage / "weather_site" / f"observation_date={day}"))
            save(build_quay_weather_site(quays, sites), "quay_weather_site", ["quay_id"])
            sections = spark.read.parquet(str(stage / "road_section" / f"observation_date={day}"))
            spec = StructType([StructField(n, t) for n, t in (("section_id", StringType()), ("latitude", DoubleType()), ("longitude", DoubleType()))])
            points = sections.select("id", "path").mapInArrow(arrow_decoder(section_points, spec), spec)
            save(near(quays, points, ["section_id"], 500).withColumn("dq_issue", F.array().cast("array<string>")), "quay_road_section", ["quay_id", "section_id"])
            quays.unpersist()
        record["contract_errors"] = {k: n for k, n in record["tables"].get("datex_quarantine", {}).get("issues", {}).items() if k in CONTRACT_ISSUES}
        record["finished_at"] = iso(utcnow())
        record["elapsed_s"] = round(time.monotonic() - started, 3)
        record["status"] = "complete" if complete and not record["contract_errors"] else "partial"
        record["ready"] = record["status"] == "complete"
        if record["contract_errors"] and not allow_partial:
            raise ValueError(f"source contract errors: {record['contract_errors']}")
        atomic_json(runs / f"{rid}.json", record)
        publish(stage, root, record, day, runs=runs)
        return record
    except BaseException as error:
        record.update(status="failed", ready=False, error=f"{type(error).__name__}: {error}",
                      finished_at=iso(utcnow()), elapsed_s=round(time.monotonic() - started, 3))
        atomic_json(runs / f"{rid}.json", record)
        raise
    finally:
        if stage.exists():
            shutil.rmtree(stage)
        if owned_spark and spark is not None:
            spark.stop()


def main(argv=None):
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", type=Date.fromisoformat, default=(utcnow() - timedelta(days=1)).date(),
                        help="UTC observation day, YYYY-MM-DD (default: yesterday)")
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
