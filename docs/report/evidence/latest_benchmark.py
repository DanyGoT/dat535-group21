"""The final state of a call two ways, on one decoded ET day: Silver's max_by against a window.

Silver keeps the newest version of every stop call with one max_by aggregate
(pipeline/silver.py, latest). The usual alternative ranks versions with row_number over a window.
This runs both over the same call versions with the same ordering, checks that they keep the same
versions, and prints time, shuffle and spill per run as JSON on stdout:

    python latest_benchmark.py <silver/_decoded/et/source_date=D> > latest-benchmark-D.json

Spark is set up as Entur Silver sets it up (local[4], 4 GB driver, 32 shuffle partitions).
Run it alone: it is one more Spark job on the machine.
"""

import contextlib
import io
import json
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "pipeline"))

from silver import CALL_KEY, latest, silver_spark  # noqa: E402


def calls(spark, folder):
    """Call versions as build_calls hands them to latest()."""
    from pyspark.sql import functions as F

    versions = spark.read.parquet(str(folder))
    return (
        versions.filter(F.col("kind") == "call")
        # Before normalize_journeys: the dated alias and the derived day stand in for the keys.
        .withColumn("journey_id", F.coalesce("journey_id", "dated_journey_id"))
        .withColumn("operating_date", F.coalesce("operating_date", "derived_operating_date"))
        .drop("kind", "entity_json", "source_date", "journey_cancelled", "extra_journey")
    )


def ranked(df, key):
    """The same choice as latest(df, key, actual_first=True), with row_number over a window."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    arrived = F.col("actual_arrival_at").isNotNull() | F.col("actual_departure_at").isNotNull()
    order = [arrived, F.col("recorded_at"), F.col("received_at"), F.col("payload_sha256"),
             F.col("entity_index")]
    # Descending puts nulls last, as max_by ranks a null below any value.
    newest = Window.partitionBy(*key).orderBy(*[o.desc() for o in order])
    return df.withColumn("_rank", F.row_number().over(newest)).filter("_rank = 1").drop("_rank")


def plan(df):
    """The physical plan's operator tree, from explain('formatted')."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        df.explain("formatted")
    tree = out.getvalue().split("\n\n")[0]
    return [line for line in tree.splitlines() if line.strip()]


def stages(spark):
    """Completed stages so far, by id, from Spark's status API."""
    context = spark.sparkContext
    url = f"{context.uiWebUrl}/api/v1/applications/{context.applicationId}/stages"
    return {s["stageId"]: s for s in json.load(urllib.request.urlopen(url))
            if s["status"] == "COMPLETE"}


def timed(spark, df):
    """Run df to the end without writing output. Runs are sequential, so every stage that
    completes meanwhile is this run's (adaptive execution submits some as separate jobs)."""
    before = set(stages(spark))
    started = time.monotonic()
    df.write.format("noop").mode("overwrite").save()
    seconds = round(time.monotonic() - started, 1)
    new = [s for i, s in stages(spark).items() if i not in before]
    total = lambda name: sum(s[name] for s in new)  # noqa: E731
    return {
        "seconds": seconds,
        "shuffle_write_bytes": total("shuffleWriteBytes"),
        "shuffle_write_records": total("shuffleWriteRecords"),
        "memory_spilled_bytes": total("memoryBytesSpilled"),
        "disk_spilled_bytes": total("diskBytesSpilled"),
    }


def fingerprint(df):
    """Rows and an order-free checksum of which version was kept per call."""
    from pyspark.sql import functions as F

    kept = F.xxhash64(*CALL_KEY, "payload_sha256", "entity_index", "recorded_at", "received_at")
    row = df.agg(F.count("*").alias("rows"), F.sum(kept).alias("checksum")).first()
    return row["rows"], row["checksum"]


def main(folder):
    folder = Path(folder)
    with tempfile.TemporaryDirectory() as scratch:
        spark = silver_spark("latest-benchmark", Path(scratch) / "out", "local[4]", "32")
        variants = {
            "max_by": lambda df: latest(df, CALL_KEY, actual_first=True),
            "row_number_window": lambda df: ranked(df, CALL_KEY),
        }
        runs = []
        # Alternate the variants, three times each. Every run reads the input afresh: running
        # the same DataFrame twice would reuse its shuffle and skip the expensive half.
        for attempt in (1, 2, 3):
            for name, build in variants.items():
                runs.append(dict(variant=name, attempt=attempt,
                                 **timed(spark, build(calls(spark, folder)))))
        versions = calls(spark, folder)
        variants = {name: build(versions) for name, build in variants.items()}
        kept = {name: fingerprint(df) for name, df in variants.items()}
        record = {
            "input": str(folder),
            "decoder_stamp": json.loads((folder / "_decoded.json").read_text()),
            "call_versions": versions.count(),
            "spark": {"master": "local[4]", "driver_memory": "4g", "shuffle_partitions": 32,
                      "version": spark.version},
            "runs": runs,
            "same_versions_kept": len(set(kept.values())) == 1,
            "kept": {name: {"rows": rows, "checksum": checksum}
                     for name, (rows, checksum) in kept.items()},
            "plans": {name: plan(df) for name, df in variants.items()},
        }
        spark.stop()
    json.dump(record, sys.stdout, indent=2, default=str)
    print()


if __name__ == "__main__":
    main(sys.argv[1])
