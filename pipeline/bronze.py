#!/usr/bin/env python3
"""Bronze: closed hourly SIRI landing files -> one compacted Parquet dataset per feed and day.

Reads   $DAT535_LANDING_DIR/siri-<feed>/date=D/hour=HH.jsonl.gz  (and legacy hour=HH.jsonl)
Writes  $DAT535_OUTPUT_DIR/bronze/siri-<feed>/date=D/*.parquet
        $DAT535_OUTPUT_DIR/bronze/_runs/date=D/<run_id>.json   (one record per run)

The raw XML payload is kept unchanged as a string; parsing it is Silver's job. Lines that
are not valid JSON, or lack a field, are kept with `_status` set, never dropped.
Rerunning a date replaces that date only, so reruns and late reruns are safe.
Landing is read-only here: open (*.part) and in-progress (*.tmp) files are never read.

Usage:
    python pipeline/bronze.py                    # yesterday (UTC)
    python pipeline/bronze.py --date 2026-10-15
    python pipeline/bronze.py --date 2026-10-15 --feeds et
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

FEEDS = ("et", "sx")
HOURS = [f"{h:02d}" for h in range(24)]
CLOSED = re.compile(r"^hour=(\d{2})\.jsonl(\.gz)?$")
OPEN = re.compile(r"^hour=(\d{2})\.jsonl\.part$")


def utcnow():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def run_id():
    """GitHub run id + attempt in CI, so a rerun of the same workflow is distinguishable."""
    gh = os.environ.get("GITHUB_RUN_ID")
    if gh:
        return f"gh-{gh}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    return f"local-{utcnow():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def scan_day(landing, feed, day):
    """Closed files of one feed and UTC day, plus which hours are missing or still open.

    Returns (files sorted by hour, missing hours, open hours). Raises if an hour has both a
    .jsonl and a .jsonl.gz, which the receiver never produces: a human must look at it.
    """
    folder = Path(landing) / f"siri-{feed}" / f"date={day}"
    closed, open_hours = {}, []
    if folder.is_dir():
        for path in sorted(folder.iterdir()):
            if m := CLOSED.match(path.name):
                if m.group(1) in closed:
                    raise RuntimeError(f"two closed files for one hour: {closed[m.group(1)]} and {path}")
                closed[m.group(1)] = path
            elif m := OPEN.match(path.name):
                open_hours.append(m.group(1))
    missing = [h for h in HOURS if h not in closed]
    return [closed[h] for h in sorted(closed)], missing, open_hours


def create_spark(app_name="DAT535-Bronze"):
    from pyspark.sql import SparkSession
    # Local mode runs everything in the driver JVM; the 1g default is too small for
    # payloads of up to ~15 MB per line across several parallel tasks.
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g")) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ingest_feed(spark, files, feed, day, out_root, rid, ingested_at):
    """Read the closed hour files of one feed/day and overwrite that day's Bronze partition.

    Returns {"rows": n, "bad_rows": {status: n}}.
    """
    from pyspark.sql.functions import col, input_file_name, lit, regexp_extract, sha2, when
    from pyspark.sql.types import StringType, StructField, StructType

    # Explicit schema: the line format is ours (written by the receiver), so no inference.
    schema = StructType([
        StructField("received_at", StringType()),
        StructField("endpoint", StringType()),
        StructField("payload", StringType()),
        StructField("_raw_line", StringType()),
    ])
    raw = spark.read \
        .schema(schema) \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_raw_line") \
        .option("encoding", "UTF-8") \
        .json([str(f) for f in files])

    status = when(col("_raw_line").isNotNull(), "corrupt") \
        .when(col("received_at").isNull() | col("payload").isNull(), "incomplete") \
        .when(col("endpoint") != feed, "wrong_endpoint") \
        .otherwise("ok")

    bronze = raw \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("hour", regexp_extract(col("_source_file"), r"hour=(\d{2})\.jsonl", 1)) \
        .withColumn("feed", lit(feed)) \
        .withColumn("_payload_sha256", sha2(col("payload"), 256)) \
        .withColumn("_status", status) \
        .withColumn("_ingested_at", lit(ingested_at)) \
        .withColumn("_run_id", lit(rid)) \
        .withColumn("date", lit(str(day))) \
        .select("received_at", "endpoint", "payload", "feed", "hour", "_payload_sha256",
                "_status", "_raw_line", "_source_file", "_ingested_at", "_run_id", "date")

    # No cache and no coalesce: a day of ET is ~200 GB of raw lines. Gzip is not splittable,
    # so Spark reads one task per hour file and writes about one Parquet file per hour.
    target = Path(out_root) / f"siri-{feed}"
    bronze.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(str(target))

    # Count from the written Parquet: reads only the _status column instead of re-parsing.
    written = spark.read.parquet(str(target / f"date={day}"))
    counts = {r["_status"]: r["count"] for r in written.groupBy("_status").count().collect()}
    return {"rows": sum(counts.values()),
            "bad_rows": {k: v for k, v in counts.items() if k != "ok"}}


def write_run_record(out_root, day, rid, record):
    """Driver-side JSON, one file per run; atomic rename so readers never see half a file."""
    folder = Path(out_root) / "_runs" / f"date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    final = folder / f"{rid}.json"
    tmp = final.with_name(final.name + ".tmp")
    tmp.write_text(json.dumps(record, indent=1) + "\n")
    os.replace(tmp, final)
    return final


def run(landing, output_dir, day, feeds=FEEDS, spark=None):
    """Bronze for one UTC day. Returns the run record (also written under _runs/)."""
    out_root = Path(output_dir) / "bronze"
    rid = run_id()
    started = utcnow()
    record = {"run_id": rid, "env": os.environ.get("DAT535_ENV", "local"), "date": str(day),
              "landing": str(landing), "started_at": iso(started), "feeds": {}}
    for feed in feeds:
        files, missing, open_hours = scan_day(landing, feed, day)
        entry = {"files": len(files), "missing_hours": missing, "open_hours": open_hours}
        if open_hours:
            log.warning("siri-%s %s: hours still open %s; rerun once they close", feed, day, open_hours)
        if not files:
            log.warning("siri-%s %s: no closed files, skipped", feed, day)
            entry.update(rows=0, bad_rows={}, skipped=True)
        else:
            if spark is None:
                spark = create_spark()
            entry.update(ingest_feed(spark, files, feed, day, out_root, rid, iso(started)))
            log.info("siri-%s %s: %d files, %d rows, bad %s, missing hours %s",
                     feed, day, len(files), entry["rows"], entry["bad_rows"], missing or "none")
        record["feeds"][feed] = entry
    record["finished_at"] = iso(utcnow())
    path = write_run_record(out_root, day, rid, record)
    log.info("run record: %s", path)
    return record


def main(argv=None):
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--date", type=Date.fromisoformat,
                   default=(utcnow() - timedelta(days=1)).date(),
                   help="UTC day to ingest, YYYY-MM-DD (default: yesterday)")
    p.add_argument("--feeds", default=",".join(FEEDS),
                   help="comma-separated feeds (default: et,sx)")
    args = p.parse_args(argv)
    feeds = [f.strip() for f in args.feeds.split(",") if f.strip()]
    unknown = set(feeds) - set(FEEDS)
    if unknown:
        p.error(f"unknown feeds: {sorted(unknown)}")

    if find_spec("pyspark") is None:
        log.error("pyspark is not installed (activate ~/spark-env)")
        return 1
    landing = Path(os.environ.get("DAT535_LANDING_DIR", "landing"))
    output_dir = Path(os.environ.get("DAT535_OUTPUT_DIR", "output"))
    if not landing.is_dir():
        log.error("landing directory %s does not exist", landing)
        return 1
    run(landing, output_dir, args.date, feeds)
    return 0


if __name__ == "__main__":
    sys.exit(main())
