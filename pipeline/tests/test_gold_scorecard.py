"""Offline Gold semantics and publication checks over small Silver-shaped fixtures."""

import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gold_scorecard as G

DAY = date(2026, 10, 26)
AT = datetime(2026, 10, 26, 8, tzinfo=timezone.utc)
CALL_SCHEMA = """operating_date date, line_id string, journey_id string, call_order int,
    has_plan boolean, has_realtime boolean, journey_cancelled boolean, call_cancelled boolean,
    for_boarding boolean, actual_departure_at timestamp, aimed_departure_at timestamp,
    departure_delay_s double, actual_departure_equals_aimed boolean, dq_issue array<string>"""
JOURNEY_SCHEMA = """operating_date date, line_id string, journey_id string,
    has_plan boolean, has_realtime boolean, journey_cancelled boolean, service_alteration string"""
LINE_SCHEMA = "operating_date date, id string, name string, public_code string, operator_id string, mode string"
OPERATOR_SCHEMA = "operating_date date, id string, name string"


def call(journey, order=1, delay=None, planned=True, reported=True, line="T:Line:1", **extra):
    result = dict(operating_date=DAY, line_id=line, journey_id=journey, call_order=order,
                  has_plan=planned, has_realtime=reported, journey_cancelled=False, call_cancelled=False,
                  for_boarding=True, actual_departure_at=AT + timedelta(seconds=delay) if delay is not None else None,
                  aimed_departure_at=AT, departure_delay_s=float(delay) if delay is not None else None,
                  actual_departure_equals_aimed=delay == 0 if delay is not None else None, dq_issue=[])
    result.update(extra)
    return result


def journey(jid, planned=True, reported=True, line="T:Line:1", **extra):
    result = dict(operating_date=DAY, line_id=line, journey_id=jid, has_plan=planned,
                  has_realtime=reported, journey_cancelled=False, service_alteration=None)
    result.update(extra)
    return result


class GoldScorecardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master("local[2]").appName("gold-scorecard-tests") \
            .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self, calls=None, journeys=None):
        return {
            "stop_call": self.spark.createDataFrame(calls if calls is not None else [call("j1", delay=180)], CALL_SCHEMA),
            "journey": self.spark.createDataFrame(journeys if journeys is not None else [journey("j1")], JOURNEY_SCHEMA),
            "line": self.spark.createDataFrame([(DAY, "T:Line:1", "Example route", "3", "T:Operator:1", "bus")], LINE_SCHEMA),
            "operator": self.spark.createDataFrame([(DAY, "T:Operator:1", "Example operator")], OPERATOR_SCHEMA),
        }

    def build(self, frames):
        return G.build_scorecard(*(frames[t] for t in G.TABLES))

    def write_silver(self, root, frames, status="complete", ready=True, rid="fixture-silver"):
        from pyspark.sql import functions as F
        counts = {}
        for table, frame in frames.items():
            frame = frame.withColumn("_silver_run_id", F.lit(rid)).withColumn("_silver_schema_version", F.lit(1))
            # Real reference snapshots may carry their date only in the path.
            frame.drop("operating_date").write.mode("overwrite").parquet(str(root / "silver" / table / f"operating_date={DAY}"))
            counts[table] = {"rows": frame.count()}
        marker = dict(run_id=rid, schema_version=1, operating_date=str(DAY), status=status, ready=ready, tables=counts)
        G.atomic_json(root / "silver" / "_runs" / f"operating_date={DAY}" / "latest.json", marker)

    def test_grains_rates_and_unknowns(self):
        frames = self.frames(
            [call("j1", delay=0), call("j1", order=2, delay=180),
             call("j2", reported=False), call("j3", planned=False, delay=-60),
             call("j4", delay=600, journey_cancelled=True)],
            [journey("j1"), journey("j2", reported=False), journey("j3", planned=False),
             journey("j4", journey_cancelled=True)])
        row = self.build(frames).first()
        self.assertEqual(row.input_journeys, 4)  # Two calls at j1 must not double its trip count.
        self.assertEqual(row.input_calls, 5)
        self.assertEqual(row.planned_journeys, 3)
        self.assertEqual(row.reported_planned_journeys, 2)
        self.assertEqual(row.unreported_planned_journeys, 1)
        self.assertEqual(row.realtime_only_journeys, 1)
        self.assertEqual(row.reported_cancelled_journeys, 1)
        self.assertEqual(row.scored_departures, 3)
        self.assertEqual(row.late_departures, 1)  # Exactly 180 seconds is late.
        self.assertEqual(row.early_departures, 1)
        self.assertEqual(row.late_departure_pct, 33.33)
        self.assertEqual(row.journey_reporting_pct, 66.67)
        self.assertEqual(row.actual_equals_aimed_departures, 1)
        self.assertEqual(row.line_name, "Example route")
        self.assertEqual(row.operator_name, "Example operator")

    def test_zero_measurements_and_planned_cancellations(self):
        frames = self.frames([call("j1", reported=False)],
                             [journey("j1", reported=False, service_alteration="cancellation"),
                              journey("j2", reported=False, service_alteration="replaced")])
        row = self.build(frames).first()
        self.assertIsNone(row.late_departure_pct)
        self.assertIsNone(row.p90_departure_delay_s)
        self.assertEqual(row.journey_reporting_pct, 0)
        self.assertEqual(row.unreported_planned_journeys, 2)
        self.assertEqual(row.reported_cancelled_journeys, 0)
        self.assertEqual(row.plan_cancellation_journeys, 1)
        self.assertEqual(row.plan_replaced_journeys, 1)

    def test_relevant_quality_and_boarding_rules(self):
        frames = self.frames([call("j1", delay=60, dq_issue=["unknown_quay"]),
                             call("j1", order=2, delay=180, dq_issue=["departure_before_arrival"]),
                             call("j1", order=3, delay=180, for_boarding=False),
                             call("j1", order=4, delay=180, call_cancelled=True),
                             call("j1", order=5, delay=-33825, dq_issue=["implausible_departure_delay"])])
        row = self.build(frames).first()
        self.assertEqual(row.scored_departures, 1)
        self.assertEqual(row.flagged_calls, 3)
        self.assertEqual(row.late_departure_pct, 0)

    def test_unknown_line_is_retained_and_duplicate_labels_fail(self):
        frames = self.frames([call("j1", delay=60, line=None)], [journey("j1", line=None)])
        row = self.build(frames).first()
        self.assertEqual(row.line_id, G.UNKNOWN_LINE)
        self.assertIsNone(row.line_name)
        self.assertEqual(row.scored_departures, 1)
        frames["line"] = frames["line"].unionByName(frames["line"])
        with self.assertRaisesRegex(ValueError, "duplicate primary key"):
            self.build(frames)

    def test_partial_gate_publication_and_rerun(self):
        with tempfile.TemporaryDirectory(prefix="gold-scorecard-test-") as temp:
            root = Path(temp)
            self.write_silver(root, self.frames(), status="partial", ready=False)
            with self.assertRaisesRegex(ValueError, "Silver is partial"):
                G.run(root, DAY, spark=self.spark)
            record = G.run(root, DAY, spark=self.spark, allow_partial=True)
            self.assertFalse(record["ready"])
            folder = root / "gold" / "line_scorecard" / f"operating_date={DAY}"
            self.assertIn("PARTIAL SAMPLE", (folder / "_preview" / "report.md").read_text())
            self.assertTrue((folder / "_preview" / "scorecard.csv").exists())
            self.assertEqual(self.spark.read.parquet(str(folder)).count(), 1)
            before = (root / "gold" / "_runs" / f"operating_date={DAY}" / "latest.json").read_text()
            self.write_silver(root, self.frames([call("j1", delay=0)]), rid="replacement-silver")
            result = G.run(root, DAY, spark=self.spark)
            self.assertTrue(result["ready"])
            self.assertEqual(self.spark.read.parquet(str(folder)).first().late_departures, 0)
            # A source marker/table mismatch fails before changing the published Gold day.
            marker = root / "silver" / "_runs" / f"operating_date={DAY}" / "latest.json"
            source = json.loads(marker.read_text())
            source["run_id"] = "wrong-run"
            G.atomic_json(marker, source)
            latest = root / "gold" / "_runs" / f"operating_date={DAY}" / "latest.json"
            good_marker = latest.read_text()
            self.assertNotEqual(before, good_marker)
            with self.assertRaisesRegex(ValueError, "disagree"):
                G.run(root, DAY, spark=self.spark)
            self.assertEqual(latest.read_text(), good_marker)
            source["status"] = "publishing"
            G.atomic_json(marker, source)
            with self.assertRaisesRegex(ValueError, "not published"):
                G.source_record(root, DAY, allow_partial=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
