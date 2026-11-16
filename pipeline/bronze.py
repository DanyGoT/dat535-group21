#!/usr/bin/env python3
"""Bronze: closed hourly landing files of SIRI ET/SX and vehicles -> one Parquet dataset per day.

Payloads stay unchanged strings (parsing is Silver's job). Bad lines are kept and flagged in
_bronze_status, never dropped. Rerunning a date replaces that date only.

    python pipeline/bronze.py                     # yesterday (UTC)
    python pipeline/bronze.py --date 2026-10-15 --feeds et,vehicles
    python pipeline/bronze.py --hours 08          # only 08:00-09:00 UTC (the dev run)

Reads  $DAT535_LANDING_DIR/<dataset>/date=D/hour=HH.jsonl.gz   (never *.part or *.tmp)
Writes $DAT535_OUTPUT_DIR/bronze/<dataset>/date=D/*.parquet and bronze/_runs/date=D/<run>.json
"""

import argparse
import json
import logging
import os
import re
import sys
import uuid
from datetime import date as Date, datetime, timedelta, timezone
from importlib.util import find_spec
from pathlib import Path

if find_spec("findspark") is not None:
    import findspark

    findspark.init()

log = logging.getLogger("bronze")

FEEDS = ("et", "sx", "vehicles")
# Landing/Bronze folder per feed (receiver.py writes siri-*, vehicles.py writes vehicles).
DATASETS = {"et": "siri-et", "sx": "siri-sx", "vehicles": "vehicles"}
HOURS = [f"{h:02d}" for h in range(24)]
CLOSED = re.compile(r"^hour=(\d{2})\.jsonl(\.gz)?$")
OPEN = re.compile(r"^hour=(\d{2})\.jsonl\.part$")

# Spark's JSON reader marks strings over 20M characters corrupt by default. Real payloads are
# longer (ET pushes of 22.8M, every DATEX situation snapshot ~28.6M), so lift the cap.
MAX_STRING_LEN = 2**31 - 1


def utcnow():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def run_id():
    """GitHub run id + attempt in CI (so reruns differ), else a local timestamp id."""
    gh = os.environ.get("GITHUB_RUN_ID")
    if gh:
        return f"gh-{gh}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    return f"local-{utcnow():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def parse_hours(text):
    """'08,09' -> ['08', '09']: two-digit UTC hours, sorted, no duplicates."""
    hours = sorted({f"{int(h):02d}" for h in text.split(",") if h.strip()})
    if not hours or any(h not in HOURS for h in hours):
        raise ValueError(f"hours must be 0-23, comma-separated: {text!r}")
    return hours


def scan_day(landing, dataset, day, hours=HOURS):
    """Closed landing files of one dataset and day. Returns (files, missing hours, open hours)."""
    folder = Path(landing) / dataset / f"date={day}"
    closed, open_hours = {}, []
    if folder.is_dir():
        for path in sorted(folder.iterdir()):
            m = CLOSED.match(path.name) or OPEN.match(path.name)
            if not m or m.group(1) not in hours:
                continue
            hour = m.group(1)
            if not CLOSED.match(path.name):
                open_hours.append(hour)
                continue
            # .jsonl and .jsonl.gz for one hour: the collectors never do that; a human must look.
            if hour in closed:
                raise RuntimeError(f"two closed files for one hour: {closed[hour]} and {path}")
            closed[hour] = path
    missing = [h for h in hours if h not in closed]
    return [closed[h] for h in sorted(closed)], missing, open_hours


def create_spark(app_name="DAT535-Bronze", settings=None):
    from pyspark.sql import SparkSession

    builder: SparkSession.Builder = SparkSession.builder  # pyright: ignore[reportAssignmentType]
    builder = (
        builder.appName(app_name)
        # Local mode runs everything in the driver; 1g is too small for ~15 MB lines.
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # zstd: one ET hour became 558 MB, against 1,621 MB with snappy, in the same time.
        .config("spark.sql.parquet.compression.codec", "zstd")
        # 256 rows per read batch: the default 4096 killed a 3 GB driver on multi-MB payloads.
        .config("spark.sql.parquet.columnarReaderBatchSize", "256")
        # Spark 3.5.0 bug SPARK-45282: a join with a cached frame could lose matches.
        .config("spark.sql.optimizer.canChangeCachedPlanOutputPartitioning", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    for name, value in (settings or {}).items():
        builder = builder.config(name, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_landing(spark, files, fields):
    """Read hour files with an explicit schema. A non-JSON line lands in _bronze_raw_line."""
    from pyspark.sql.types import StringType, StructField, StructType

    # The line format is ours (collector/), so nothing is inferred.
    columns = [StructField(name, kind) for name, kind in fields]
    schema = StructType(columns + [StructField("_bronze_raw_line", StringType())])
    return (
        spark.read.schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_bronze_raw_line")
        .option("encoding", "UTF-8")
        .option("maxStringLen", MAX_STRING_LEN)
        .json([str(f) for f in files])
    )


def with_evidence(raw, feed, status, rid, ingested_at):
    """The landed columns unchanged, plus source file, payload hash and size, status and run."""
    from pyspark.sql.functions import (
        col,
        input_file_name,
        lit,
        octet_length,
        regexp_extract,
        sha2,
    )

    landed = [c for c in raw.columns if c != "_bronze_raw_line"]
    hour = regexp_extract(col("_bronze_source_file"), r"hour=(\d{2})\.jsonl", 1)
    return (
        raw.withColumn("_bronze_source_file", input_file_name())
        .withColumn("hour", hour)
        .withColumn("feed", lit(feed))
        .withColumn("_bronze_payload_sha256", sha2(col("payload"), 256))
        .withColumn("_bronze_payload_bytes", octet_length(col("payload")))
        .withColumn("_bronze_status", status)
        .withColumn("_bronze_ingested_at", lit(ingested_at))
        .withColumn("_bronze_run_id", lit(rid))
        .select(
            *landed,
            "feed",
            "hour",
            "_bronze_payload_sha256",
            "_bronze_payload_bytes",
            "_bronze_status",
            "_bronze_raw_line",
            "_bronze_source_file",
            "_bronze_ingested_at",
            "_bronze_run_id",
        )
    )


def status_counts(written):
    """{_bronze_status: rows} of a written partition (reads one narrow column, no payloads)."""
    rows = written.groupBy("_bronze_status").count().collect()
    return {r["_bronze_status"]: r["count"] for r in rows}


def ingest_feed(spark, files, feed, day, out_root, rid, ingested_at):
    """Overwrite one feed's Bronze day from its hour files. Returns {rows, bad_rows}."""
    from pyspark.sql.functions import col, when
    from pyspark.sql.types import StringType

    fields = [("received_at", StringType()), ("endpoint", StringType()), ("payload", StringType())]
    raw = read_landing(spark, files, fields)

    missing_field = col("received_at").isNull() | col("endpoint").isNull() | col("payload").isNull()
    status = (
        when(col("_bronze_raw_line").isNotNull(), "corrupt")
        .when(missing_field, "incomplete")
        .when(col("endpoint") != feed, "wrong_endpoint")
        .otherwise("ok")
    )
    bronze = with_evidence(raw, feed, status, rid, ingested_at)

    # No cache, no coalesce: an ET day is ~200 GB of raw lines. Gzip can't be split, so it is
    # one task and about one Parquet file per hour. Written straight into the day's folder:
    # partitionBy("date") would sort, and spill, the whole day first.
    partition = Path(out_root) / DATASETS[feed] / f"date={day}"
    bronze.write.mode("overwrite").parquet(str(partition))

    counts = status_counts(spark.read.parquet(str(partition)))
    bad_rows = {k: v for k, v in counts.items() if k != "ok"}
    return {"rows": sum(counts.values()), "bad_rows": bad_rows}


def write_run_record(out_root, day, rid, record, name=None):
    """Write _runs/date=D/<name or run id>.json atomically, so readers never see half a file."""
    folder = Path(out_root) / "_runs" / f"date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    final = folder / f"{name or rid}.json"
    tmp = final.with_name(final.name + ".tmp")
    tmp.write_text(json.dumps(record, indent=1) + "\n")
    os.replace(tmp, final)
    return final


def run(landing, output_dir, day, feeds=FEEDS, spark=None, hours=HOURS):
    """Bronze for one UTC day (or some of its hours). Returns the run record."""
    out_root = Path(output_dir) / "bronze"
    rid = run_id()
    started = utcnow()
    record = {
        "run_id": rid,
        "env": os.environ.get("DAT535_ENV", "local"),
        "date": str(day),
        "landing": str(landing),
        "started_at": iso(started),
        "feeds": {},
    }
    if hours != HOURS:
        record["hours"] = list(hours)

    for feed in feeds:
        dataset = DATASETS[feed]
        files, missing, open_hours = scan_day(landing, dataset, day, hours)
        entry = {"files": len(files), "missing_hours": missing, "open_hours": open_hours}
        if open_hours:
            log.warning("%s %s: hours still open %s; rerun once closed", dataset, day, open_hours)

        if not files:
            log.warning("%s %s: no closed files, skipped", dataset, day)
            entry.update(rows=0, bad_rows={}, skipped=True)
        else:
            if spark is None:
                spark = create_spark()
            entry.update(ingest_feed(spark, files, feed, day, out_root, rid, iso(started)))
            log.info(
                "%s %s: %d files, %d rows, bad %s, missing hours %s",
                dataset, day, len(files), entry["rows"], entry["bad_rows"], missing or "none",
            )
        record["feeds"][feed] = entry

    record["finished_at"] = iso(utcnow())
    path = write_run_record(out_root, day, rid, record)
    log.info("run record: %s", path)
    return record


def cli(doc, feeds, run, logger, argv=None):
    """Command line of both Bronze jobs: --date, --feeds and --hours, then run."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    p = argparse.ArgumentParser(description=doc.splitlines()[0])
    p.add_argument(
        "--date",
        type=Date.fromisoformat,
        default=(utcnow() - timedelta(days=1)).date(),
        help="UTC day to ingest, YYYY-MM-DD (default: yesterday)",
    )
    p.add_argument(
        "--feeds",
        default=",".join(feeds),
        help=f"comma-separated feeds (default: {','.join(feeds)})",
    )
    p.add_argument(
        "--hours",
        type=parse_hours,
        default=HOURS,
        help="comma-separated UTC hours, e.g. 08,09 (default: the whole day)",
    )
    args = p.parse_args(argv)
    chosen = [f.strip() for f in args.feeds.split(",") if f.strip()]
    unknown = set(chosen) - set(feeds)
    if unknown:
        p.error(f"unknown feeds: {sorted(unknown)}")

    if find_spec("pyspark") is None:
        logger.error("pyspark is not installed (activate ~/spark-env)")
        return 1
    landing = Path(os.environ.get("DAT535_LANDING_DIR", "landing"))
    output_dir = Path(os.environ.get("DAT535_OUTPUT_DIR", "output"))
    if not landing.is_dir():
        logger.error("landing directory %s does not exist", landing)
        return 1
    run(landing, output_dir, args.date, chosen, hours=args.hours)
    return 0


def main(argv=None):
    return cli(__doc__, FEEDS, run, log, argv)


if __name__ == "__main__":
    sys.exit(main())
