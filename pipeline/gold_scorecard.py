#!/usr/bin/env python3
"""Daily line reliability scorecard over published Entur Silver, with a Markdown preview.

One row per operating date and line. Partial Silver requires --allow-partial and
produces an explicitly non-ready example, never a complete-day reliability claim.
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
from datetime import date as Date
from pathlib import Path

from bronze import create_spark, iso, run_id, utcnow
from silver import assert_unique, atomic_json, publish

TABLES = ("stop_call", "journey", "line", "operator")
KEY = ["operating_date", "line_id"]
UNKNOWN_LINE = "__unknown_line__"


def source_record(output, day, allow_partial=False):
    path = Path(output) / "silver" / "_runs" / f"operating_date={day}" / "latest.json"
    record = json.loads(path.read_text())
    if record.get("schema_version") != 1 or record.get("operating_date") != str(day):
        raise ValueError("Silver marker has an unsupported schema or wrong operating date")
    if record.get("status") not in {"complete", "partial"}:
        raise ValueError("Silver is not published: refusing a failed or publishing marker")
    if not (record.get("status") == "complete" and record.get("ready") is True) and not allow_partial:
        raise ValueError("Silver is partial; use --allow-partial only for an exploratory example")
    return record


def build_scorecard(calls, journeys, lines, operators, late_seconds=180):
    """Aggregate each fact grain separately, then attach daily reference labels."""
    from pyspark.sql import functions as F

    def count_if(condition, name):
        return F.sum(F.when(condition, 1).otherwise(0)).alias(name)

    def grouped(frame):
        return frame.withColumn("line_id", F.coalesce("line_id", F.lit(UNKNOWN_LINE)))

    def percent(numerator, denominator):
        return F.when(F.col(denominator) > 0, F.round(100.0 * F.col(numerator) / F.col(denominator), 2))

    assert_unique(lines, ["operating_date", "id"], "Gold line reference")
    assert_unique(operators, ["operating_date", "id"], "Gold operator reference")
    planned, reported = F.col("has_plan"), F.col("has_realtime")
    cancelled = F.coalesce(F.col("journey_cancelled"), F.lit(False))
    trip_counts = grouped(journeys).groupBy(*KEY).agg(
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

    cancelled = cancelled | F.coalesce(F.col("call_cancelled"), F.lit(False))
    delay = F.col("departure_delay_s")
    # A terminal call can have an actual departure even when boarding is forbidden.
    # Missing coordinates/labels do not make otherwise usable timing evidence invalid.
    scored = reported & ~cancelled & F.coalesce(F.col("for_boarding"), F.lit(True)) & \
        F.col("actual_departure_at").isNotNull() & F.col("aimed_departure_at").isNotNull() & \
        delay.isNotNull() & ~F.isnan(delay) & \
        ~F.array_contains("dq_issue", "departure_before_arrival") & \
        ~F.array_contains("dq_issue", "implausible_departure_delay")
    call_counts = grouped(calls).groupBy(*KEY).agg(
        F.count("*").alias("input_calls"),
        count_if(planned, "planned_calls"),
        count_if(reported, "reported_calls"),
        count_if(planned & reported, "reported_planned_calls"),
        count_if(planned & ~reported, "unreported_planned_calls"),
        count_if(~planned & reported, "realtime_only_calls"),
        count_if(reported & cancelled, "reported_cancelled_calls"),
        count_if(F.size("dq_issue") > 0, "flagged_calls"),
        count_if(scored, "scored_departures"),
        count_if(scored & (delay >= late_seconds), "late_departures"),
        count_if(scored & (delay < 0), "early_departures"),
        count_if(scored & F.col("actual_departure_equals_aimed"), "actual_equals_aimed_departures"),
        F.percentile_approx(F.when(scored, delay), [0.5, 0.9], 10000).alias("_delay_percentiles"),
    )
    count_columns = [c for c in trip_counts.columns + call_counts.columns if c not in KEY + ["_delay_percentiles"]]
    result = trip_counts.join(call_counts, KEY, "full").fillna(0, subset=count_columns)
    result = result.withColumn("journey_reporting_pct", percent("reported_planned_journeys", "planned_journeys")) \
        .withColumn("call_reporting_pct", percent("reported_planned_calls", "planned_calls")) \
        .withColumn("late_departure_pct", percent("late_departures", "scored_departures")) \
        .withColumn("median_departure_delay_s", F.col("_delay_percentiles")[0]) \
        .withColumn("p90_departure_delay_s", F.col("_delay_percentiles")[1]).drop("_delay_percentiles")
    labels = lines.select("operating_date", F.col("id").alias("line_id"),
                          F.col("name").alias("line_name"), "public_code", "operator_id", "mode")
    operator_labels = operators.select("operating_date", F.col("id").alias("operator_id"),
                                       F.col("name").alias("operator_name"))
    return result.join(F.broadcast(labels), KEY, "left") \
        .join(F.broadcast(operator_labels), ["operating_date", "operator_id"], "left")


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
    from pyspark.sql import functions as F

    output = Path(output_dir)
    source = source_record(output, day, allow_partial)
    root = output / "gold"
    rid = run_id()
    stage = root / "_staging" / (rid + "-" + uuid.uuid4().hex[:8])
    history = root / "_runs" / f"operating_date={day}" / f"{rid}.json"
    owned_spark = spark is None
    started = time.monotonic()
    record = dict(run_id=rid, operating_date=str(day), schema_version=1, status="running", ready=False,
                  started_at=iso(utcnow()), silver_run_id=source["run_id"], silver_status=source["status"],
                  late_seconds=180, tables={})
    try:
        if spark is None:
            scratch = output.parent / "spark-scratch"
            scratch.mkdir(parents=True, exist_ok=True)
            spark = create_spark("DAT535-Gold-Scorecard-Example", settings={
                "spark.master": "local[2]", "spark.driver.memory": "2g",
                "spark.sql.shuffle.partitions": "4", "spark.local.dir": str(scratch)})
        frames = {}
        for table in TABLES:
            folder = output / "silver" / table / f"operating_date={day}"
            if not (folder / "_SUCCESS").exists():
                raise ValueError(f"Silver partition is not complete: {folder}")
            # Partition keys can live only in directory names, especially in
            # reference snapshots. Keep the table root for Spark's discovery.
            frame = spark.read.option("basePath", str(output / "silver" / table)).parquet(str(folder))
            wrong_source = ~F.col("_silver_run_id").eqNullSafe(F.lit(source["run_id"])) | \
                ~F.col("_silver_schema_version").eqNullSafe(F.lit(1)) | \
                ~F.col("operating_date").eqNullSafe(F.lit(day))
            audit = frame.agg(F.count("*").alias("rows"),
                              F.sum(wrong_source.cast("long")).alias("wrong_source")).first()
            if audit["wrong_source"] or audit["rows"] != source["tables"][table]["rows"]:
                raise ValueError(f"{table}: rows disagree with the published Silver run")
            frames[table] = frame
        scorecard = build_scorecard(*(frames[t] for t in TABLES))
        partition = stage / "line_scorecard" / f"operating_date={day}"
        scorecard = scorecard.withColumn("_gold_run_id", F.lit(rid)) \
            .withColumn("_source_silver_run_id", F.lit(source["run_id"]))
        scorecard.write.parquet(str(partition))
        written = spark.read.parquet(str(partition))
        assert_unique(written, KEY, "Gold line scorecard")
        # Only the small line/day aggregate reaches the driver, never stop-call facts.
        rows = [r.asDict() for r in written.orderBy("line_id").collect()]
        totals = {c: sum(r[c] for r in rows) for c in (
            "input_journeys", "input_calls", "planned_journeys", "reported_journeys", "scored_departures",
            "late_departures", "actual_equals_aimed_departures")}
        if totals["input_journeys"] != source["tables"]["journey"]["rows"] or \
                totals["input_calls"] != source["tables"]["stop_call"]["rows"]:
            raise ValueError("Gold totals do not reconcile to Silver; refusing publication")
        ready = source["status"] == "complete" and source["ready"] is True
        record.update(totals=totals, status="complete" if ready else "partial", ready=ready,
                      finished_at=iso(utcnow()), elapsed_s=round(time.monotonic() - started, 3))
        record["tables"]["line_scorecard"] = dict(rows=len(rows), partition=f"operating_date={day}",
            parquet_bytes=sum(p.stat().st_size for p in partition.glob("*.parquet")))
        # Spark ignores underscore directories when discovering Parquet files.
        preview = partition / "_preview"
        preview.mkdir()
        with (preview / "scorecard.csv").open("w", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=written.columns)
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
    parser.add_argument("--allow-partial", action="store_true", help="create an explicitly non-ready exploration report")
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
