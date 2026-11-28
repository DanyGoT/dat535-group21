"""How many pairs Gold's transfer join compares, against pairing every arrival and departure
of a hub. Reads one day of Silver and prints JSON on stdout:

    python transfer_pairs.py <root with silver/stop_call, silver/quay, silver/stop_place>

The sides are those of gold.build_transfers before its cancellation and delay columns:
arrivals that can alight after the first call, departures that can board before the last,
both with an aimed time and a known hub. Run it alone: it is one more Spark job on the machine.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "pipeline"))

import gold as G  # noqa: E402
from silver import silver_spark  # noqa: E402


def main(root):
    from pyspark.sql import functions as F

    spark = silver_spark("transfer-pairs", Path(root) / "tmp-out", "local[4]", "32")
    read = lambda table: spark.read.parquet(f"{root}/silver/{table}")  # noqa: E731
    quay_hub, _ = G.build_hubs(read("quay"), read("stop_place"))
    calls = G.hub_calls(read("stop_call").filter(F.col("line_id").isNotNull()), quay_hub)
    calls = calls.filter(F.col("hub_id") != G.UNKNOWN_HUB)

    aimed = lambda kind: F.coalesce(f"aimed_{kind}_at", f"planned_{kind}_at").cast("long")  # noqa: E731
    arrivals = calls.filter(
        (F.col("call_order") > F.col("first_order"))
        & F.coalesce(F.col("for_alighting"), F.lit(True))
        & aimed("arrival").isNotNull()
    ).select("hub_id", aimed("arrival").alias("a_at")).cache()
    departures = calls.filter(
        (F.col("call_order") < F.col("last_order"))
        & F.coalesce(F.col("for_boarding"), F.lit(True))
        & aimed("departure").isNotNull()
    ).select("hub_id", aimed("departure").alias("b_at")).cache()

    # Every arrival with every departure of its hub: what a join on hub alone would compare.
    per_hub = (arrivals.groupBy("hub_id").count().withColumnRenamed("count", "a")
               .join(departures.groupBy("hub_id").count().withColumnRenamed("count", "b"), "hub_id"))
    hub_wide = per_hub.agg(F.sum(F.col("a") * F.col("b"))).first()[0]

    # Gold's equi-join on 15-minute buckets.
    bucket = F.floor(F.col("a_at") / G.MAX_SLACK_S)
    joined = (arrivals.withColumn("_bucket", F.explode(F.array(bucket, bucket + 1)))
              .join(departures.withColumn("_bucket", F.floor(F.col("b_at") / G.MAX_SLACK_S)),
                    ["hub_id", "_bucket"]))
    within = (F.col("b_at") - F.col("a_at")).between(0, G.MAX_SLACK_S)
    record = {
        "input": str(root),
        "arrivals": arrivals.count(),
        "departures": departures.count(),
        "hub_wide_pairs": hub_wide,
        "bucket_candidates": joined.count(),
        "within_15_min": joined.filter(within).count(),
    }
    spark.stop()
    json.dump(record, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main(sys.argv[1])
