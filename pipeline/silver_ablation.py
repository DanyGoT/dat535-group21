#!/usr/bin/env python3
"""Is Silver a good base for Gold? Ablation test on the departure-risk question.

Not a product. For sampled stop calls: "will this departure be at least 3 min late, asked
10 min before it?" One gradient-boosted model per set of feature groups, each group coming from
one Silver source, trained on earlier days and tested on later ones. A source that carries
signal lowers the error when it is added and raises it when it is removed; a source that
carries none, or whose joins are wrong, does not. Every feature uses only what had arrived by
the lookup time: the checks in build_features fail the run otherwise.

    python pipeline/silver_ablation.py --profile-days 2026-10-29,... --train-days ... --test-days ...

Reads silver/ under $DAT535_OUTPUT_DIR, writes silver/_ablation/<run>/ (features and results).
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from bronze import create_spark, iso, run_id, utcnow

log = logging.getLogger("silver_ablation")
BUCKET_S = 600
ROAD_INCIDENTS = ("Accident", "VehicleObstruction", "GeneralObstruction", "AnimalPresenceObstruction", "EnvironmentalObstruction",
                  "InfrastructureDamageObstruction", "PoorEnvironmentConditions", "WeatherRelatedRoadConditions", "NonWeatherRelatedRoadConditions")
ROAD_WORKS = ("MaintenanceWorks", "ConstructionWorks")
GROUPS = {
    "calendar": ["hour_local", "day_of_week", "is_weekend", "mode_index", "call_order", "is_first_call"],
    "history": ["hist_late_rate", "hist_calls", "line_late_rate"],
    "realtime": ["predicted_delay_s", "has_prediction"],
    "weather": ["air_temperature_c", "road_surface_temperature_c", "precipitation_mm_h", "wind_speed", "friction", "visibility_m",
                "slippery_road", "site_distance_km", "has_weather"],
    "situations": ["situations_active", "incidents_active", "works_active", "closures_active", "severe_active"],
    "congestion": ["congestion_ratio", "congestion_max", "has_congestion"],
    "alerts": ["line_alerts_active"],
}
# Set of groups per model. "all" minus one group is the ablation; the rest shows what each adds.
VARIANTS = {
    "calendar": ["calendar"],
    "calendar+history": ["calendar", "history"],
    "calendar+realtime": ["calendar", "realtime"],
    "calendar+history+realtime": ["calendar", "history", "realtime"],
    "all": list(GROUPS),
    **{f"all-{group}": [g for g in GROUPS if g != group] for group in GROUPS},
}


def functions():
    from pyspark.sql import functions as F
    return F


def bucket(column):
    """The 10-minute boundary at or after a time: when something that arrived then can be used."""
    F = functions()
    return (F.ceil(F.col(column).cast("double") / BUCKET_S) * BUCKET_S).cast("long")


def scored_calls(spark, silver, day, percent):
    """Reported departures with a usable outcome, whole journeys sampled by hash."""
    F = functions()
    calls = spark.read.parquet(f"{silver}/stop_call/operating_date={day}")
    cancelled = F.coalesce("journey_cancelled", F.lit(False)) | F.coalesce("call_cancelled", F.lit(False))
    usable = calls.filter(F.col("has_realtime") & F.col("actual_departure_at").isNotNull() & F.col("aimed_departure_at").isNotNull() & ~cancelled
                          & F.coalesce("for_boarding", F.lit(True)) & ~F.array_contains("dq_issue", "implausible_departure_delay")
                          & ~F.array_contains("dq_issue", "departure_before_arrival"))
    if percent < 100:
        usable = usable.filter(F.pmod(F.hash("journey_id"), F.lit(100)) < percent)
    local = F.from_utc_timestamp(F.coalesce("planned_departure_at", "aimed_departure_at"), "Europe/Oslo")
    return usable.select(
        F.col("operating_date").cast("string").alias("day"), "journey_id", "call_order", "line_id", "quay_id", "mode",
        F.coalesce("planned_departure_at", "aimed_departure_at").alias("target_at"), "departure_delay_s",
        (F.col("departure_delay_s") >= 180).cast("int").alias("label"),
        F.hour(local).alias("hour_local"), F.dayofweek(local).alias("day_of_week"), F.dayofweek(local).isin(1, 7).cast("int").alias("is_weekend"),
        (F.col("call_order") == 1).cast("int").alias("is_first_call"))


def build_profile(spark, silver, days):
    """Late share per line, quay, hour and day type from the profile days only, shrunk towards
    the line's own share when a stop has few observations."""
    F = functions()
    calls = None
    for day in days:
        frame = scored_calls(spark, silver, day, 100)
        calls = frame if calls is None else calls.unionByName(frame)
    lines = calls.groupBy("line_id").agg(F.avg("label").alias("line_late_rate"))
    stops = calls.groupBy("line_id", "quay_id", "hour_local", "is_weekend").agg(F.count("*").alias("hist_calls"), F.sum("label").alias("_late"))
    return stops.join(lines, "line_id") \
        .withColumn("hist_late_rate", (F.col("_late") + 10 * F.col("line_late_rate")) / (F.col("hist_calls") + 10)).drop("_late"), lines


def weather_at(spark, silver, day):
    """Per station and 10-minute boundary, the newest measurement that had arrived by then."""
    from pyspark.sql import Window
    F = functions()
    weather = spark.read.parquet(f"{silver}/road_weather/observation_date={day}").filter(~F.array_contains("dq_issue", "stale_measurement"))
    flagged = lambda column, issue: F.when(~F.array_contains("dq_issue", issue), F.col(column))
    rows = weather.select(
        "site_id", bucket("first_received_at").alias("bucket"), "measurement_at",
        flagged("air_temperature_c", "suspect_air_temperature").alias("air_temperature_c"), "road_surface_temperature_c",
        flagged("precipitation_mm_h", "suspect_precipitation").alias("precipitation_mm_h"), flagged("wind_speed", "suspect_wind").alias("wind_speed"),
        flagged("friction", "suspect_friction").alias("friction"), "visibility_m",
        F.col("road_condition").isin("snow", "ice", "slushOnRoad", "frost", "blackIce").cast("int").alias("slippery_road"))
    newest = Window.partitionBy("site_id", "bucket").orderBy(F.col("measurement_at").desc())
    return rows.withColumn("_rank", F.row_number().over(newest)).filter(F.col("_rank") == 1).drop("_rank", "measurement_at")


def situations_at(spark, silver, day):
    """Per quay and 10-minute boundary, how many situations nearby were known and applied then."""
    F = functions()
    links = spark.read.parquet(f"{silver}/quay_road_situation/observation_date={day}").select("quay_id", "record_id", "version", "version_at")
    intervals = spark.read.parquet(f"{silver}/road_situation_interval/observation_date={day}")
    seen = spark.read.parquet(f"{silver}/road_situation/observation_date={day}").select("record_id", "version", "version_at", "first_seen_at", "last_seen_at")
    key = ["record_id", "version", "version_at"]
    known = intervals.join(seen, key).withColumn("_from", (F.ceil(F.greatest("start_at", "first_seen_at").cast("double") / BUCKET_S) * BUCKET_S).cast("long")) \
        .withColumn("_to", (F.floor(F.least("end_at", "last_seen_at").cast("double") / BUCKET_S) * BUCKET_S).cast("long")) \
        .filter(F.col("_from") <= F.col("_to")) \
        .withColumn("bucket", F.explode(F.sequence("_from", "_to", F.lit(BUCKET_S).cast("long"))))
    closed = F.exists("record_subtypes", lambda s: s.isin("roadClosed", "carriagewayClosures", "laneClosures"))
    joined = known.join(links, key)
    return joined.groupBy("quay_id", "bucket").agg(
        F.count("*").alias("situations_active"),
        F.sum(F.col("record_type").isin(*ROAD_INCIDENTS).cast("int")).alias("incidents_active"),
        F.sum(F.col("record_type").isin(*ROAD_WORKS).cast("int")).alias("works_active"),
        F.sum(closed.cast("int")).alias("closures_active"),
        F.sum(F.col("severity").isin("high", "highest").cast("int")).alias("severe_active"))


def congestion_at(spark, silver, day):
    F = functions()
    links = spark.read.parquet(f"{silver}/quay_road_section/observation_date={day}").select("quay_id", "section_id")
    times = spark.read.parquet(f"{silver}/road_travel_time/observation_date={day}").filter(F.col("has_measurement"))
    return times.select("section_id", bucket("first_received_at").alias("bucket"), "congestion_ratio").join(links, "section_id") \
        .groupBy("quay_id", "bucket").agg(F.avg("congestion_ratio").alias("congestion_ratio"), F.max("congestion_ratio").alias("congestion_max"))


def alerts(spark, silver, day):
    """SX situations naming a line, with when they applied and when we first saw them. SX pushes on
    change, so a long-running alert may have arrived before this day: the two days before count too."""
    from datetime import date, timedelta
    F = functions()
    start = date.fromisoformat(day)
    folders = [f"{silver}/sx_situation/observation_date={start - timedelta(days=n)}" for n in (2, 1, 0)]
    sx = spark.read.option("basePath", f"{silver}/sx_situation").parquet(*[f for f in folders if Path(f, "_SUCCESS").exists()])
    return sx.select("situation_id", "first_received_at", F.explode("validity_periods").alias("period"), "affects") \
        .select("situation_id", "first_received_at", F.col("period.start_at").alias("start_at"), F.col("period.end_at").alias("end_at"), F.explode("affects").alias("entity")) \
        .filter(F.col("entity.entity_type") == "LineRef").select("situation_id", "first_received_at", "start_at", "end_at", F.col("entity.entity_id").alias("line_id")).distinct()


def build_features(spark, silver, day, percent, lead, profile, lines, modes):
    F = functions()
    calls = scored_calls(spark, silver, day, percent).withColumn("lookup_at", F.expr(f"target_at - INTERVAL {lead} MINUTES")) \
        .withColumn("bucket", (F.floor(F.col("lookup_at").cast("double") / BUCKET_S) * BUCKET_S).cast("long"))
    calls = calls.join(F.broadcast(modes), "mode", "left")
    predictions = spark.read.parquet(f"{silver}/call_prediction/operating_date={day}") \
        .filter((F.col("event") == "departure") & (F.col("reference") == "planned") & (F.col("lead_minutes") == lead)) \
        .select("journey_id", "call_order", (F.col("predicted_at").cast("double") - F.col("target_at").cast("double")).alias("predicted_delay_s"),
                F.col("available_at").alias("_prediction_known_at"), F.col("cutoff_at").alias("_cutoff_at"))
    out = calls.join(predictions, ["journey_id", "call_order"], "left").withColumn("has_prediction", F.col("predicted_delay_s").isNotNull().cast("int"))
    out = out.join(profile, ["line_id", "quay_id", "hour_local", "is_weekend"], "left") \
        .join(F.broadcast(lines.withColumnRenamed("line_late_rate", "_line_rate")), "line_id", "left") \
        .withColumn("line_late_rate", F.coalesce("line_late_rate", "_line_rate")).drop("_line_rate")
    sites = spark.read.parquet(f"{silver}/quay_weather_site/observation_date={day}").select("quay_id", "site_id", (F.col("distance_m") / 1000).alias("site_distance_km"))
    out = out.join(F.broadcast(sites), "quay_id", "left").join(weather_at(spark, silver, day), ["site_id", "bucket"], "left") \
        .withColumn("has_weather", F.col("air_temperature_c").isNotNull().cast("int"))
    out = out.join(situations_at(spark, silver, day), ["quay_id", "bucket"], "left") \
        .fillna(0, subset=["situations_active", "incidents_active", "works_active", "closures_active", "severe_active"])
    out = out.join(congestion_at(spark, silver, day), ["quay_id", "bucket"], "left").withColumn("has_congestion", F.col("congestion_ratio").isNotNull().cast("int"))
    active = alerts(spark, silver, day)
    counted = calls.select("journey_id", "call_order", "line_id", "lookup_at").join(F.broadcast(active), "line_id") \
        .filter((F.col("first_received_at") <= F.col("lookup_at")) & (F.col("start_at").isNull() | (F.col("start_at") <= F.col("lookup_at")))
                & (F.col("end_at").isNull() | (F.col("lookup_at") <= F.col("end_at")))) \
        .groupBy("journey_id", "call_order").agg(F.countDistinct("situation_id").alias("line_alerts_active"))
    return calls, out.join(counted, ["journey_id", "call_order"], "left").fillna(0, subset=["line_alerts_active"])


def write_checked(spark, calls, features, path, day):
    """The promises that make this a fair test, checked on what was written: nothing known after
    its cutoff, and no join changed the number of calls."""
    F = functions()
    features.write.mode("overwrite").parquet(path)
    written = spark.read.parquet(path)
    leaks = written.filter(F.col("_prediction_known_at") > F.col("_cutoff_at")).count()
    if leaks:
        raise ValueError(f"{day}: {leaks} predictions known after their cutoff")
    rows, distinct = written.count(), written.select("journey_id", "call_order").distinct().count()
    expected = calls.count()
    if rows != expected or rows != distinct:
        raise ValueError(f"{day}: feature joins changed the grain: {expected} calls, {rows} rows, {distinct} keys")
    return rows


def evaluate(predictions):
    F = functions()
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.functions import vector_to_array
    auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC").evaluate(predictions)
    scored = predictions.select("label", vector_to_array("probability")[1].alias("p")).withColumn("p", F.least(F.greatest("p", F.lit(1e-6)), F.lit(1 - 1e-6)))
    row = scored.agg(F.avg((F.col("p") - F.col("label")) ** 2).alias("brier"),
                     F.avg(-(F.col("label") * F.log("p") + (1 - F.col("label")) * F.log(1 - F.col("p")))).alias("log_loss")).first()
    return dict(auc=round(auc, 4), brier=round(row["brier"], 4), log_loss=round(row["log_loss"], 4))


def stratify(test):
    """Sanity: does each joined source move the late share the way it should? No model."""
    F = functions()
    out = {}
    for name, condition in (("slippery road", F.col("slippery_road") == 1), ("dry or wet road", F.col("slippery_road") == 0),
                            ("precipitation >= 1 mm/h", F.col("precipitation_mm_h") >= 1), ("no precipitation", F.col("precipitation_mm_h") == 0),
                            ("incident within 500 m", F.col("incidents_active") > 0), ("no situation nearby", F.col("situations_active") == 0),
                            ("road works within 500 m", F.col("works_active") > 0), ("road closure within 500 m", F.col("closures_active") > 0),
                            ("congested road (ratio >= 1.5)", F.col("congestion_max") >= 1.5), ("free road (ratio < 1.1)", F.col("congestion_max") < 1.1),
                            ("alert on the line", F.col("line_alerts_active") > 0), ("no alert on the line", F.col("line_alerts_active") == 0),
                            ("Entur predicts >= 3 min late", F.col("predicted_delay_s") >= 180), ("Entur predicts < 3 min late", F.col("predicted_delay_s") < 180)):
        row = test.filter(condition).agg(F.count("*").alias("calls"), F.avg("label").alias("late"), F.expr("percentile_approx(departure_delay_s, 0.5, 1000)").alias("median")).first()
        out[name] = dict(calls=row["calls"], late_pct=round(100 * row["late"], 1) if row["late"] is not None else None, median_delay_s=row["median"])
    return out


def main(argv=None):
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--profile-days", required=True)
    p.add_argument("--train-days", required=True)
    p.add_argument("--test-days", required=True)
    p.add_argument("--percent", type=int, default=20, help="journeys sampled, percent")
    p.add_argument("--lead", type=int, default=10, help="minutes before the planned departure")
    p.add_argument("--trees", type=int, default=40)
    p.add_argument("--variants", default=",".join(VARIANTS))
    args = p.parse_args(argv)
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import GBTClassifier
    from pyspark.ml.feature import VectorAssembler
    F = functions()
    output = Path(os.environ.get("DAT535_OUTPUT_DIR", "output"))
    silver = output / "silver"
    profile_days, train_days, test_days = (args.profile_days.split(","), args.train_days.split(","), args.test_days.split(","))
    if max(profile_days) >= min(train_days) or max(train_days) >= min(test_days):
        p.error("days must be in time order: profile < train < test")
    spark = create_spark("DAT535-Silver-Ablation", settings={"spark.master": os.environ.get("SPARK_MASTER", "local[3]"),
                                                             "spark.local.dir": str(output.parent / "spark-scratch"),
                                                             "spark.sql.shuffle.partitions": "16"})
    rid = run_id()
    work = silver / "_ablation" / rid
    result = dict(run_id=rid, started_at=iso(utcnow()), profile_days=profile_days, train_days=train_days, test_days=test_days, percent=args.percent,
                  lead_minutes=args.lead, trees=args.trees, models={})
    modes = spark.createDataFrame([(m, i) for i, m in enumerate(("bus", "tram", "metro", "rail", "water", "coach", "air", "cableway", "funicular"))], "mode string, mode_index int")
    profile, lines = build_profile(spark, str(silver), profile_days)
    profile = profile.cache()
    lines = lines.cache()
    for split, days in (("train", train_days), ("test", test_days)):
        for day in days:
            started = time.monotonic()
            calls, features = build_features(spark, str(silver), day, args.percent, args.lead, profile, lines, modes)
            rows = write_checked(spark, calls, features, str(work / split / f"day={day}"), day)
            log.info("features %s %s: %d calls in %.0f s", split, day, rows, time.monotonic() - started)
    profile.unpersist()
    lines.unpersist()
    names = [c for g in GROUPS.values() for c in g]
    train = spark.read.parquet(str(work / "train")).drop("_prediction_known_at", "_cutoff_at").fillna(-999, subset=names).cache()
    test = spark.read.parquet(str(work / "test")).drop("_prediction_known_at", "_cutoff_at").cache()
    result["rows"] = dict(train=train.count(), test=test.count())
    result["late_share"] = dict(train=train.agg(F.avg("label")).first()[0], test=test.agg(F.avg("label")).first()[0])
    result["coverage"] = {c: test.agg(F.avg(c)).first()[0] for c in ("has_prediction", "has_weather", "has_congestion")}
    result["coverage"]["any_situation"] = test.agg(F.avg((F.col("situations_active") > 0).cast("double"))).first()[0]
    result["coverage"]["history"] = test.agg(F.avg(F.col("hist_calls").isNotNull().cast("double"))).first()[0]
    result["stratified"] = stratify(test)
    atomic = work / "result.json"
    atomic.write_text(json.dumps(result, indent=1, default=str))
    filled = test.fillna(-999, subset=names).cache()
    # What a rule can do without a model: Entur's own estimate, thresholded.
    entur = filled.filter(F.col("has_prediction") == 1).withColumn("hit", (F.col("predicted_delay_s") >= 180).cast("int"))
    result["entur_rule"] = entur.agg(F.count("*").alias("calls"), F.avg((F.col("hit") == F.col("label")).cast("double")).alias("accuracy"),
                                     (F.sum(F.col("hit") * F.col("label")) / F.sum("hit")).alias("precision"),
                                     (F.sum(F.col("hit") * F.col("label")) / F.sum("label")).alias("recall")).first().asDict()
    for name in args.variants.split(","):
        groups = VARIANTS[name]
        columns = [c for g in groups for c in GROUPS[g]]
        started = time.monotonic()
        pipeline = Pipeline(stages=[VectorAssembler(inputCols=columns, outputCol="features", handleInvalid="keep"),
                                    GBTClassifier(labelCol="label", featuresCol="features", maxIter=args.trees, maxDepth=5, seed=535, cacheNodeIds=True, maxBins=64)])
        model = pipeline.fit(train)
        scored = model.transform(filled)
        metrics = evaluate(scored)
        importances = dict(sorted(zip(columns, (round(float(x), 4) for x in model.stages[-1].featureImportances.toArray())), key=lambda kv: -kv[1])[:8])
        result["models"][name] = dict(metrics, groups=groups, seconds=round(time.monotonic() - started), top_features=importances)
        log.info("%s: %s", name, result["models"][name])
        atomic.write_text(json.dumps(result, indent=1, default=str))
    result["finished_at"] = iso(utcnow())
    atomic.write_text(json.dumps(result, indent=1, default=str))
    print(json.dumps(result, indent=1, default=str))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
