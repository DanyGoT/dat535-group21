#!/usr/bin/env python3
"""DATEX Silver: one UTC day of polled snapshots -> road situations, weather and travel times.

--date is the UTC day of the polls (DATEX has no operating day). Run after that day's Bronze.
Same machinery as Entur Silver; marker: silver/_runs/datex/observation_date=D/latest.json.
Contracts and readiness: docs/pipeline/silver.md.
"""

import logging
import os
import shutil
import sys
import time
import uuid
from functools import partial
from pathlib import Path

from bronze import HOURS, iso, run_id, utcnow
from silver import (
    CONTRACT_ISSUES,
    atomic_json,
    audit_bronze,
    cli,
    empty_issues,
    finish,
    functions,
    issues,
    publish,
    silver_spark,
    stage_table,
    write_decoded,
)
from silver_datex_parse import decode_partition, schema, section_points, situation_points
from silver_parse import arrow_decoder

log = logging.getLogger("silver_datex")

SCHEMA_VERSION = 1
SITUATION_KEY = ["record_id", "version", "version_at"]
STREAMS = ("situation", "traveltime", "weather")
REFERENCES = ("traveltime-locations", "weather-sites")

# Per feed: the kind of row it publishes, and that row's key.
KEYS = {
    "situation": ("record", SITUATION_KEY),
    "weather": ("value", ["site_id", "measurement_at"]),
    "traveltime": ("value", ["section_id", "period_start_at"]),
    "weather-sites": ("site", ["id"]),
    "traveltime-locations": ("section", ["id"]),
}


def read_bronze(spark, output, feed, day, allow_partial, record):
    """The day's polls of one feed. Returns (frame or None, complete)."""
    F = functions()
    folder = Path(output) / "bronze" / f"datex-{feed}" / f"date={day}"
    if not (folder / "_SUCCESS").exists():
        record["inputs"][feed] = dict(missing_partition=True)
        if not allow_partial:
            raise ValueError(
                f"no Bronze for datex-{feed} on {day}; use --allow-partial only for exploration"
            )
        return None, False

    frame = spark.read.parquet(str(folder))
    statuses, hours, run_ids = audit_bronze(frame)
    # Location tables are polled four times a day: only the streams must cover every hour.
    missing = sorted(set(HOURS) - hours) if feed in STREAMS else []
    ok = statuses.get("ok", 0) + statuses.get("not_modified", 0)
    record["inputs"][feed] = dict(
        missing_hours=missing,
        rows=sum(statuses.values()),
        bronze_status=statuses,
        bronze_run_ids=run_ids,
    )
    complete = not missing and ok > 0
    if not complete and not allow_partial:
        raise ValueError(
            f"incomplete datex-{feed} Bronze: {record['inputs'][feed]}; "
            "use --allow-partial only for exploration"
        )
    polls = frame.withColumn("source_date", F.lit(str(day))).select(
        "received_at", "payload", "_bronze_source_file", "_bronze_payload_sha256",
        "_bronze_run_id", "_bronze_status", "source_date",
    )
    return polls, complete


def decode(spark, source, feed, work):
    """Decode one feed's polls to typed rows."""
    # A situation snapshot is ~27 MB: hold one at a time. Four per batch filled a 3 GB heap
    # on 2026-11-03 (docs/report/problems.md).
    write_decoded(spark, source, partial(decode_partition, feed=feed), schema(feed), work, 1)
    return spark.read.parquet(str(work))


def split_unkeyed(frame, feed):
    """Split into (rows with their whole key, quarantine rows). Nothing vanishes silently."""
    F = functions()
    kind, key = KEYS[feed]
    unkeyed = F.exists(F.array(*[F.col(k).isNull() for k in key]), lambda x: x)

    # Unkeyed rows of the published kind go to quarantine, with the reason and the row as JSON.
    # (Unkeyed sightings are just dropped: the record they belong to is quarantined.)
    reasons = [(f"missing_{k}", F.col(k).isNull()) for k in key]
    row_json = F.to_json(F.struct(*[c for c in frame.columns if c != "entity_json"]))
    bad = issues(frame.filter((F.col("kind") == kind) & unkeyed), *reasons)
    bad = bad.withColumn("entity_json", row_json)
    bad = frame.filter(F.col("kind") == "quarantine").unionByName(bad)

    good = frame.filter((F.col("kind") != "quarantine") & ~unkeyed)
    bad = bad.select(
        F.lit(feed).alias("feed"),
        "received_at", "source_file", "payload_sha256", "bronze_run_id", "dq_issue", "entity_json",
    )
    return good, bad


def first_and_last(df, key):
    """One row per key with its earliest copy's values, plus when it was first and last seen."""
    F = functions()
    values = [c for c in df.columns if c not in key]
    earliest = F.min_by(F.struct(*values), F.struct("received_at", "payload_sha256"))
    return (
        df.groupBy(*key)
        .agg(
            earliest.alias("_earliest"),
            F.min("received_at").alias("first_received_at"),
            F.max("received_at").alias("last_received_at"),
        )
        .select(*key, "_earliest.*", "first_received_at", "last_received_at")
    )


def build_situations(parsed):
    """One row per situation record version, with when and how often the polls saw it."""
    F = functions()
    key = SITUATION_KEY
    sightings = (
        parsed.filter(F.col("kind") == "sighting")
        .groupBy(*key)
        .agg(
            F.min("received_at").alias("first_seen_at"),
            F.max("received_at").alias("last_seen_at"),
            F.count("*").alias("snapshots_seen"),
        )
    )
    records = first_and_last(parsed.filter(F.col("kind") == "record"), key)
    records = records.drop("first_received_at", "last_received_at")
    situations = records.join(sightings, key).drop("kind", "entity_json")

    def outside_norway(loc):
        return ~loc.latitude.between(57, 72) | ~loc.longitude.between(3, 33)

    return issues(
        situations,
        ("missing_start", F.col("start_at").isNull()),
        ("end_before_start", F.col("end_at") < F.col("start_at")),
        ("version_before_created", F.col("version_at") < F.col("created_at")),
        ("missing_location", F.size("locations") == 0),
        ("coordinates_outside_norway", F.exists("locations", outside_norway)),
    )


def build_intervals(situations):
    """One row per time span in the day during which a situation record applied."""
    F = functions()
    columns = SITUATION_KEY + ["record_type", "record_subtypes", "severity", "dq_issue"]
    return (
        situations.select(*columns, F.explode("active_intervals").alias("interval"))
        .select(*columns, "interval.start_at", "interval.end_at")
    )


def build_weather(parsed):
    """One row per station and measurement time, from the first poll that carried it."""
    F = functions()
    values = parsed.filter(F.col("kind") == "value")
    weather = first_and_last(values, ["site_id", "measurement_at"]).drop("kind", "entity_json")

    air = F.col("air_temperature_c")
    surface = F.col("road_surface_temperature_c")
    humidity = F.col("relative_humidity_pct")
    wind = F.col("wind_speed")
    # Over an hour old when first received: a frozen station (some were 1.6 years behind).
    age_s = F.col("first_received_at").cast("long") - F.col("measurement_at").cast("long")

    # Bounds from what we saw on 2026-11-05: air -39.9 over a road above zero, humidity 0,
    # dew point -81.9, a constant 80.3 mm/h. Flagged, kept.
    return issues(
        weather,
        ("stale_measurement", age_s > 3600),
        ("suspect_air_temperature", ~air.between(-50, 40) | (F.abs(air - surface) > 30)),
        ("suspect_road_temperature", ~surface.between(-50, 70)),
        ("suspect_humidity", (humidity <= 0) | (humidity > 105)),
        ("suspect_dew_point", F.col("dew_point_c") < -60),
        ("suspect_precipitation", F.col("precipitation_mm_h") > 50),
        ("suspect_wind", (wind > 60) | (F.col("wind_gust") > 80)),
        ("suspect_friction", ~F.col("friction").between(0, 1)),
    )


def build_travel_times(parsed):
    """One row per road section and 5-minute period, from the first poll that carried it."""
    F = functions()
    values = parsed.filter(F.col("kind") == "value")
    times = (
        first_and_last(values, ["section_id", "period_start_at"])
        .drop("kind", "entity_json")
        .withColumn("has_measurement", F.col("travel_time_s").isNotNull())
        .withColumn("congestion_ratio", F.col("travel_time_s") / F.col("free_flow_s"))
    )
    status_unknown = F.col("traffic_status") == "unknown"
    return issues(
        times,
        ("nonpositive_travel_time", F.col("travel_time_s") <= 0),
        ("nonpositive_free_flow", F.col("free_flow_s") <= 0),
        ("measurement_without_status", F.col("has_measurement") & status_unknown),
    )


def latest_reference(parsed, kind):
    """Every id the day's snapshots had, each with its values from the latest one that had it."""
    F = functions()
    rows = parsed.filter(F.col("kind") == kind).drop("kind", "entity_json")
    values = [c for c in rows.columns if c != "id"]
    newest = F.max_by(F.struct(*values), F.struct("received_at", "payload_sha256"))
    return (
        rows.groupBy("id")
        .agg(newest.alias("_latest"))
        .select("id", "_latest.*")
        .select(*rows.columns)
    )


def build_sites(parsed, weather):
    """The stations plus how each behaved that day: which ones Gold may map a quay to."""
    F = functions()
    sites = latest_reference(parsed, "site")

    # A non-zero value that never changes all day is a stuck sensor. (Zero precipitation is a
    # dry day and 20,000 m is the visibility sensor's maximum, so neither is checked.)
    quantities = (
        "air_temperature_c", "road_surface_temperature_c", "relative_humidity_pct",
        "precipitation_mm_h", "wind_speed",
    )
    checked = [c for c in quantities if c in weather.columns]
    stale = F.array_contains("dq_issue", "stale_measurement")
    stats = (
        weather.filter(~stale)
        .groupBy("site_id")
        .agg(
            F.count("*").alias("measurements"),
            F.max("measurement_at").alias("last_measurement_at"),
            *[(F.count_distinct(c) == 1).alias("_one_" + c) for c in checked],
            *[F.max(c).alias("_max_" + c) for c in checked],
        )
    )

    def is_stuck(c):
        return F.col("_one_" + c) & (F.col("measurements") >= 20) & (F.col("_max_" + c) != 0)

    stuck = F.array(*[F.when(is_stuck(c), F.lit(c)) for c in checked])
    stats = (
        stats.withColumn("stuck_quantities", F.filter(stuck, lambda x: x.isNotNull()))
        .select("site_id", "measurements", "last_measurement_at", "stuck_quantities")
    )

    # A frozen station still has a last measurement time, from its stale rows.
    frozen = (
        weather.filter(stale)
        .groupBy("site_id")
        .agg(F.max("measurement_at").alias("_frozen_at"))
    )
    out = (
        sites.join(stats, sites.id == stats.site_id, "left")
        .drop("site_id")
        .join(frozen, sites.id == frozen.site_id, "left")
        .drop("site_id")
        .withColumn("last_measurement_at", F.coalesce("last_measurement_at", "_frozen_at"))
        .drop("_frozen_at")
        .fillna(0, subset=["measurements"])
        .withColumn("usable", F.col("measurements") >= 72)
    )
    no_position = F.col("latitude").isNull() | F.col("longitude").isNull()
    return issues(
        out,
        ("no_current_measurement", F.col("measurements") == 0),
        ("missing_coordinates", no_position),
    )


def build_sections(parsed, times):
    """The road sections plus how many of the day's periods each was measured in."""
    F = functions()
    sections = latest_reference(parsed, "section")
    stats = times.groupBy("section_id").agg(
        F.count("*").alias("periods"),
        F.sum(F.col("has_measurement").cast("long")).alias("measured_periods"),
    )
    out = (
        sections.join(stats, sections.id == stats.section_id, "left")
        .drop("section_id")
        .fillna(0, subset=["periods", "measured_periods"])
    )
    return issues(
        out,
        ("missing_geometry", F.col("path").isNull() | (F.size("path") < 2)),
        ("never_measured", F.col("measured_periods") == 0),
    )


def newest_quays(spark, root, day):
    """Entur Silver's newest quay table on or before the day. Returns (quays, its day) or Nones."""
    # NSR changes slowly, and Entur Silver for D runs a day after this job.
    F = functions()
    folders = sorted(
        p
        for p in (root / "quay").glob("operating_date=*")
        if p.name.removeprefix("operating_date=") <= str(day) and (p / "_SUCCESS").exists()
    )
    if not folders:
        return None, None
    has_position = F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    quays = spark.read.parquet(str(folders[-1])).filter(has_position).select(
        F.col("id").alias("quay_id"),
        F.col("latitude").alias("quay_latitude"),
        F.col("longitude").alias("quay_longitude"),
    )
    return quays, folders[-1].name.removeprefix("operating_date=")


def haversine_m(lat1, lon1, lat2, lon2):
    """Great-circle metres between two points, as a Spark column."""
    F = functions()
    d_lat = F.radians(lat2 - lat1)
    d_lon = F.radians(lon2 - lon1)
    cos_both = F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
    x = F.sin(d_lat / 2) ** 2 + cos_both * F.sin(d_lon / 2) ** 2
    return F.lit(2 * 6371000.0) * F.asin(F.sqrt(x))


def near(quays, points, key, radius_m):
    """One row per (quay, key) within radius_m, with its smallest distance.

    Both sides get a grid cell: 0.01° latitude (1.1 km) x 0.02° longitude (0.76-1.2 km).
    A point within 500 m of a quay is in the quay's cell or a neighbour, so each quay is
    compared with a few points instead of all of them.
    """
    F = functions()
    around = F.array(F.lit(-1), F.lit(0), F.lit(1))
    # Each quay in its own cell and its 8 neighbours.
    q = (
        quays.withColumn("_d_lat", F.explode(around))
        .withColumn("_d_lon", F.explode(around))
        .withColumn("_cell_lat", F.floor(F.col("quay_latitude") / 0.01) + F.col("_d_lat"))
        .withColumn("_cell_lon", F.floor(F.col("quay_longitude") / 0.02) + F.col("_d_lon"))
        .drop("_d_lat", "_d_lon")
    )
    p = (
        points.withColumn("_cell_lat", F.floor(F.col("latitude") / 0.01))
        .withColumn("_cell_lon", F.floor(F.col("longitude") / 0.02))
    )
    distance = haversine_m(
        F.col("quay_latitude"), F.col("quay_longitude"), F.col("latitude"), F.col("longitude")
    )
    return (
        q.join(p, ["_cell_lat", "_cell_lon"])
        .withColumn("distance_m", distance)
        .filter(F.col("distance_m") <= radius_m)
        .groupBy("quay_id", *key)
        .agg(F.round(F.min("distance_m"), 1).alias("distance_m"))
    )


def build_quay_weather_site(quays, sites):
    """Each quay's nearest station that worked that day. One row per quay."""
    # Road stations are inland (ferry quays: median 14 km away), so Gold decides how far is
    # too far; we only flag beyond 30 km.
    F = functions()
    usable = sites.filter(F.col("usable") & F.col("latitude").isNotNull()).select(
        F.col("id").alias("site_id"),
        F.col("latitude").alias("site_latitude"),
        F.col("longitude").alias("site_longitude"),
    )
    distance = haversine_m(
        F.col("quay_latitude"), F.col("quay_longitude"),
        F.col("site_latitude"), F.col("site_longitude"),
    )
    pairs = quays.crossJoin(F.broadcast(usable)).withColumn("distance_m", distance)
    closest = F.min_by(F.struct("site_id", "distance_m"), "distance_m")
    nearest = (
        pairs.groupBy("quay_id")
        .agg(closest.alias("nearest"))
        .select("quay_id", "nearest.site_id", F.round("nearest.distance_m", 1).alias("distance_m"))
        .withColumn("dq_issue", empty_issues())
    )
    return issues(nearest, ("nearest_site_beyond_30km", F.col("distance_m") > 30000))


def link_situations(quays, situations):
    """Situation records within 500 m of each quay, anywhere along their geometry."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

    spec = StructType([
        StructField("record_id", StringType()),
        StructField("version", StringType()),
        StructField("version_at", TimestampType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ])
    located = situations.select(*SITUATION_KEY, "locations")
    points = located.mapInArrow(arrow_decoder(situation_points, spec), spec)
    return near(quays, points, SITUATION_KEY, 500).withColumn("dq_issue", empty_issues())


def link_sections(quays, sections):
    """Travel time sections within 500 m of each quay, anywhere along the road."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    spec = StructType([
        StructField("section_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ])
    points = sections.select("id", "path").mapInArrow(arrow_decoder(section_points, spec), spec)
    return near(quays, points, ["section_id"], 500).withColumn("dq_issue", empty_issues())


def run(landing, output_dir, day, spark=None, allow_partial=False):
    root = Path(output_dir) / "silver"
    rid = run_id()
    started = time.monotonic()
    stage = root / "_staging" / (rid + "-datex-" + uuid.uuid4().hex[:8])
    runs = root / "_runs" / "datex" / f"observation_date={day}"
    record = dict(
        run_id=rid,
        source="datex",
        env=os.environ.get("DAT535_ENV", "local"),
        schema_version=SCHEMA_VERSION,
        observation_date=str(day),
        started_at=iso(utcnow()),
        status="running",
        ready=False,
        inputs={},
        tables={},
    )
    owned_spark = spark is None
    try:
        if spark is None:
            spark = silver_spark("DAT535-Silver-DATEX", output_dir, "local[3]", "4")
        modules = (
            "silver.py", "silver_parse.py", "silver_reference.py", "silver_datex_parse.py",
            "bronze.py",
        )
        for module in modules:
            spark.sparkContext.addPyFile(str(Path(__file__).with_name(module)))
        stage.mkdir(parents=True)

        # 1. Read and decode every feed.
        parsed, complete = {}, True
        for feed in STREAMS + REFERENCES:
            source, covered = read_bronze(spark, output_dir, feed, day, allow_partial, record)
            complete &= covered
            if source is None:
                continue
            parsed[feed] = decode(spark, source, feed, stage / "_work" / feed)
            kinds = parsed[feed].groupBy("kind").count().collect()
            record["inputs"][feed]["decoded"] = {r["kind"]: r["count"] for r in kinds}

        def save(df, table, key):
            written, stats = stage_table(
                df, stage, table, day, rid, key, "observation_date", schema_version=SCHEMA_VERSION
            )
            stats["partition"] = f"observation_date={day}"
            record["tables"][table] = stats
            return written

        rejected = None
        for feed in list(parsed):
            parsed[feed], bad = split_unkeyed(parsed[feed], feed)
            rejected = bad if rejected is None else rejected.unionByName(bad)

        # 2. Tables per feed. Reference tables need that day's measurements too.
        if "situation" in parsed:
            situations = build_situations(parsed["situation"])
            situations = save(situations, "road_situation", SITUATION_KEY)
            intervals = build_intervals(situations)
            save(intervals, "road_situation_interval", SITUATION_KEY + ["start_at"])

        if "weather" in parsed:
            weather = build_weather(parsed["weather"])
            weather = save(weather, "road_weather", ["site_id", "measurement_at"])
            if "weather-sites" in parsed:
                sites = build_sites(parsed["weather-sites"], weather)
                sites = save(sites, "weather_site", ["id"])

        if "traveltime" in parsed:
            times = build_travel_times(parsed["traveltime"])
            times = save(times, "road_travel_time", ["section_id", "period_start_at"])
            if "traveltime-locations" in parsed:
                sections = build_sections(parsed["traveltime-locations"], times)
                sections = save(sections, "road_section", ["id"])

        if rejected is not None:
            save(rejected, "datex_quarantine", None)

        # 3. Link quays to nearby road items (needs an Entur Silver quay table).
        quays, record["quay_snapshot_date"] = newest_quays(spark, root, day)
        if quays is None:
            log.warning("no Entur Silver quay snapshot on or before %s: spatial links skipped", day)
        elif {"road_situation", "weather_site", "road_section"} <= set(record["tables"]):
            quays = quays.cache()
            save(
                link_situations(quays, situations),
                "quay_road_situation",
                ["quay_id"] + SITUATION_KEY,
            )
            save(build_quay_weather_site(quays, sites), "quay_weather_site", ["quay_id"])
            save(link_sections(quays, sections), "quay_road_section", ["quay_id", "section_id"])
            quays.unpersist()

        # 4. Verdict, then publish.
        quarantined = record["tables"].get("datex_quarantine", {}).get("issues", {})
        record["contract_errors"] = {
            k: n for k, n in quarantined.items() if k in CONTRACT_ISSUES
        }
        finish(record, complete, allow_partial, started)
        atomic_json(runs / f"{rid}.json", record)
        publish(stage, root, record, day, runs=runs)
        return record
    except BaseException as error:
        record.update(
            status="failed",
            ready=False,
            error=f"{type(error).__name__}: {error}",
            finished_at=iso(utcnow()),
            elapsed_s=round(time.monotonic() - started, 3),
        )
        atomic_json(runs / f"{rid}.json", record)
        raise
    finally:
        if stage.exists():
            shutil.rmtree(stage)
        if owned_spark and spark is not None:
            spark.stop()


def main(argv=None):
    date_help = "UTC observation day, YYYY-MM-DD (default: yesterday)"
    return cli(__doc__, run, 1, date_help, log, argv)


if __name__ == "__main__":
    sys.exit(main())
