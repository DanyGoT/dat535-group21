"""Which operating days one UTC day of ET call versions belongs to.

Entur Silver for operating day D reads the Bronze of UTC days D-1, D and D+1, so a UTC day X is
read for D = X-1, X and X+1 only. Versions of any other day never reach Silver. This counts them,
from one decoded day (silver/_decoded/et/source_date=X), and prints JSON on stdout:

    python late_data.py <silver/_decoded/et/source_date=X>

Run it alone: it is one more Spark job on the machine.
"""

import json
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "pipeline"))

from silver import CALL_KEY, silver_spark  # noqa: E402


def main(folder):
    from pyspark.sql import functions as F

    folder = Path(folder)
    utc_day = date.fromisoformat(folder.name.split("=")[1])
    window = [str(utc_day + timedelta(days=d)) for d in (-1, 0, 1)]
    spark = silver_spark("late-data", folder.parent / "late-data-out", "local[4]", "32")
    calls = (
        spark.read.parquet(str(folder))
        .filter(F.col("kind") == "call")
        # As build_calls sees them: the dated alias and the derived day stand in for the keys.
        .withColumn("journey_id", F.coalesce("journey_id", "dated_journey_id"))
        .withColumn("operating_date", F.coalesce("operating_date", "derived_operating_date"))
    )
    actual = F.col("actual_arrival_at").isNotNull() | F.col("actual_departure_at").isNotNull()
    cancelled = (F.coalesce("call_cancelled", F.lit(False))
                 | F.coalesce("journey_cancelled", F.lit(False)))
    rows = (
        calls.groupBy(F.col("operating_date").cast("string").alias("operating_date"))
        .agg(
            F.count("*").alias("versions"),
            F.countDistinct(*CALL_KEY).alias("calls"),
            F.sum(actual.cast("int")).alias("versions_with_actual"),
            F.sum(cancelled.cast("int")).alias("versions_cancelled"),
        )
        .orderBy("operating_date")
        .collect()
    )
    days = {r["operating_date"]: r.asDict() for r in rows}
    total = sum(r["versions"] for r in rows)
    outside = [r for d, r in days.items() if d not in window]
    record = {
        "input": f"silver/_decoded/et/source_date={utc_day}",
        "read_for_operating_days": window,
        "call_versions": total,
        "outside_window": {
            "versions": sum(r["versions"] for r in outside),
            "calls": sum(r["calls"] for r in outside),
            "versions_with_actual": sum(r["versions_with_actual"] for r in outside),
            "versions_cancelled": sum(r["versions_cancelled"] for r in outside),
        },
        "by_operating_day": list(days.values()),
    }
    spark.stop()
    json.dump(record, sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main(sys.argv[1])
