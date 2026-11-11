"""Offline Gold semantics and publication checks over small Silver-shaped fixtures."""

import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gold as G

DAY = date(2026, 11, 9)
AT = datetime(2026, 11, 9, 8, tzinfo=timezone.utc)  # 09:00 in Oslo
CALL_SCHEMA = """operating_date date, line_id string, journey_id string, call_order int, quay_id string,
    mode string, has_plan boolean, has_realtime boolean, journey_cancelled boolean, call_cancelled boolean,
    for_boarding boolean, for_alighting boolean, aimed_arrival_at timestamp, planned_arrival_at timestamp,
    actual_arrival_at timestamp, aimed_departure_at timestamp, planned_departure_at timestamp,
    actual_departure_at timestamp, departure_delay_s double, actual_departure_equals_aimed boolean,
    dq_issue array<string>"""
JOURNEY_SCHEMA = """operating_date date, line_id string, journey_id string,
    has_plan boolean, has_realtime boolean, journey_cancelled boolean, service_alteration string"""
SCHEMAS = {
    "line": "operating_date date, id string, name string, public_code string, operator_id string, mode string",
    "operator": "operating_date date, id string, name string",
    "quay": "operating_date date, id string, stop_place_id string",
    "stop_place": "operating_date date, id string, name string, parent_stop_place_id string, latitude double, longitude double",
    "road_weather": "site_id string, measurement_at timestamp, precipitation_mm_h double, road_surface_temperature_c double, dq_issue array<string>",
    "weather_site": "id string, stuck_quantities array<string>",
    "road_situation_interval": "record_id string, version string, version_at timestamp, record_type string, start_at timestamp, end_at timestamp",
    "road_travel_time": "section_id string, period_start_at timestamp, has_measurement boolean, congestion_ratio double, dq_issue array<string>",
    "quay_weather_site": "quay_id string, site_id string, distance_m double",
    "quay_road_situation": "quay_id string, record_id string, version string, version_at timestamp",
    "quay_road_section": "quay_id string, section_id string",
}
QUAYS = [("Q:1", "SP:1"), ("Q:2", "SP:2"), ("Q:O", "SP:O"), ("Q:H1", "SP:H1"), ("Q:H2", "SP:H2"), ("Q:D", "SP:D")]
STOP_PLACES = [("SP:1", "Stop", None), ("SP:2", "Other stop", None), ("SP:O", "Origin", None),
               ("SP:H1", "Bus terminal", "P:H"), ("SP:H2", "Railway station", "P:H"), ("P:H", "Hub", None),
               ("SP:D", "Destination", None)]


def at(minutes):
    return AT + timedelta(minutes=minutes)


def call(journey, order=1, delay=None, planned=True, reported=True, line="T:Line:1", **extra):
    result = dict(operating_date=DAY, line_id=line, journey_id=journey, call_order=order, quay_id="Q:1",
                  mode="bus", has_plan=planned, has_realtime=reported, journey_cancelled=False,
                  call_cancelled=False, for_boarding=True, for_alighting=True, aimed_arrival_at=None,
                  planned_arrival_at=None, actual_arrival_at=None, aimed_departure_at=AT, planned_departure_at=AT,
                  actual_departure_at=AT + timedelta(seconds=delay) if delay is not None else None,
                  departure_delay_s=float(delay) if delay is not None else None,
                  actual_departure_equals_aimed=delay == 0 if delay is not None else None, dq_issue=[])
    result.update(extra)
    return result


def stop(journey, order, line, quay, arrive=None, depart=None, arrive_delay=0, depart_delay=0, **extra):
    """A reported call aimed `arrive`/`depart` minutes after AT; delays in seconds, None = no actual."""
    def actual(minutes, delay):
        return None if minutes is None or delay is None else at(minutes) + timedelta(seconds=delay)
    return call(journey, order, delay=depart_delay if depart is not None else None, line=line, quay_id=quay,
                aimed_arrival_at=None if arrive is None else at(arrive), actual_arrival_at=actual(arrive, arrive_delay),
                aimed_departure_at=None if depart is None else at(depart), planned_departure_at=None,
                actual_departure_at=actual(depart, depart_delay), **extra)


def journey(jid, planned=True, reported=True, line="T:Line:1", **extra):
    result = dict(operating_date=DAY, line_id=line, journey_id=jid, has_plan=planned,
                  has_realtime=reported, journey_cancelled=False, service_alteration=None)
    result.update(extra)
    return result


class GoldTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master("local[2]").appName("gold-tests") \
            .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self, calls=None, journeys=None):
        frame = self.spark.createDataFrame
        return {
            "stop_call": frame(calls if calls is not None else [call("j1", delay=180)], CALL_SCHEMA),
            "journey": frame(journeys if journeys is not None else [journey("j1")], JOURNEY_SCHEMA),
            "line": frame([(DAY, "T:Line:1", "Example route", "3", "T:Operator:1", "bus")], SCHEMAS["line"]),
            "operator": frame([(DAY, "T:Operator:1", "Example operator")], SCHEMAS["operator"]),
            "quay": frame([(DAY, *q) for q in QUAYS], SCHEMAS["quay"]),
            "stop_place": frame([(DAY, *s, 58.9, 5.7) for s in STOP_PLACES], SCHEMAS["stop_place"]),
        }

    def roads(self, **rows):
        return {t: self.spark.createDataFrame(rows.get(t, []), SCHEMAS[t]) for t in G.SOURCES["datex"][3]}

    def scorecard(self, frames):
        return G.build_scorecard(frames["stop_call"], frames["journey"], frames["line"], frames["operator"])

    def hubs(self, frames):
        return G.build_hubs(frames["quay"], frames["stop_place"])

    def write_silver(self, root, frames, roads, status="complete", ready=True, rid="fixture-silver"):
        from pyspark.sql import functions as F
        for source, data in (("entur", frames), ("datex", roads)):
            runs, partition, schema, _ = G.SOURCES[source]
            counts = {}
            for table, frame in data.items():
                # Real partitions carry their date only in the path.
                frame.drop("operating_date").withColumn("_silver_run_id", F.lit(rid)) \
                    .write.mode("overwrite").parquet(str(root / "silver" / table / f"{partition}={DAY}"))
                counts[table] = {"rows": frame.count()}
            marker = {"run_id": rid, "schema_version": schema, partition: str(DAY), "status": status,
                      "ready": ready, "tables": counts}
            G.atomic_json(root / "silver" / runs / f"{partition}={DAY}" / "latest.json", marker)

    def test_grains_rates_and_unknowns(self):
        frames = self.frames(
            [call("j1", delay=0), call("j1", order=2, delay=180),
             call("j2", reported=False), call("j3", planned=False, delay=-60),
             call("j4", delay=600, journey_cancelled=True)],
            [journey("j1"), journey("j2", reported=False), journey("j3", planned=False),
             journey("j4", journey_cancelled=True)])
        row = self.scorecard(frames).first()
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
        row = self.scorecard(frames).first()
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
        row = self.scorecard(frames).first()
        self.assertEqual(row.scored_departures, 1)
        self.assertEqual(row.flagged_calls, 3)
        self.assertEqual(row.late_departure_pct, 0)

    def test_unknown_line_is_retained_and_duplicate_labels_fail(self):
        frames = self.frames([call("j1", delay=60, line=None)], [journey("j1", line=None)])
        row = self.scorecard(frames).first()
        self.assertEqual(row.line_id, G.UNKNOWN_LINE)
        self.assertIsNone(row.line_name)
        self.assertEqual(row.scored_departures, 1)
        frames["line"] = frames["line"].unionByName(frames["line"])
        with self.assertRaisesRegex(ValueError, "duplicate primary key"):
            self.scorecard(frames)

    def test_transfers_pair_each_arrival_with_the_first_departure_per_line(self):
        calls = [
            # Feeder A: origin, then arrives at the hub's bus terminal 3 min late.
            stop("f1", 1, "A", "Q:O", depart=-20), stop("f1", 2, "A", "Q:H1", arrive=0, depart=1, arrive_delay=180),
            stop("f1", 3, "A", "Q:D", arrive=10),
            # B leaves the station quay of the same hub 5 min later, 1 min late: the gap shrinks by 2 min.
            stop("b1", 1, "B", "Q:H2", depart=5, depart_delay=60), stop("b1", 2, "B", "Q:D", arrive=20),
            stop("b2", 1, "B", "Q:H2", depart=10), stop("b2", 2, "B", "Q:D", arrive=25),  # not the first B
            stop("c1", 1, "C", "Q:H2", depart=20), stop("c1", 2, "C", "Q:D", arrive=40),  # beyond 15 min
            stop("e1", 1, "E", "Q:H2", depart=7, call_cancelled=True), stop("e1", 2, "E", "Q:D", arrive=30),
            stop("u1", 1, "U", "Q:H2", depart=6, depart_delay=None), stop("u1", 2, "U", "Q:D", arrive=30),
            stop("g1", 1, "A", "Q:H2", depart=3), stop("g1", 2, "A", "Q:D", arrive=30),  # same line
        ]
        frames = self.frames(calls)
        rows = {r.to_line_id: r for r in G.build_transfers(frames["stop_call"], *self.hubs(frames)).collect()}
        self.assertEqual(set(rows), {"B", "E", "U"})
        b = rows["B"]
        self.assertEqual((b.hub_id, b.from_line_id, b.from_origin_name, b.to_destination_name, b.hour),
                         ("P:H", "A", "Origin", "Destination", 9))
        self.assertEqual((b.connections, b.cancelled_connections, b.unknown_connections), (1, 0, 0))
        self.assertEqual(b.gap_change_counts[-2 - G.GAP_LOW], 1)
        self.assertEqual(sum(b.gap_change_counts), 1)
        self.assertEqual((rows["E"].cancelled_connections, sum(rows["E"].gap_change_counts)), (1, 0))
        self.assertEqual((rows["U"].unknown_connections, sum(rows["U"].gap_change_counts)), (1, 0))

    def test_conditions_and_stops_split_the_same_departures(self):
        calls = [
            stop("d1", 1, "L", "Q:1", depart=0, depart_delay=240),
            stop("d2", 1, "L", "Q:2", depart=0),
            stop("d3", 1, "L", "Q:1", depart=-510),  # 23:30 UTC the day before: outside DATEX's day
            stop("d4", 1, "L", "Q:1", depart=90),
            stop("d5", 1, "L", "Q:1", depart=30, call_cancelled=True),
        ]
        version = at(-600)
        roads = self.roads(
            road_weather=[("W1", at(10), 0.5, -1.0, []), ("W1", at(40), 0.2, 0.5, []),
                          ("W1", at(20), 90.0, -5.0, ["suspect_precipitation"])],
            weather_site=[("W1", []), ("W2", [])],
            quay_weather_site=[("Q:1", "W1", 2000.0), ("Q:2", "W2", 15000.0)],
            quay_road_situation=[("Q:1", "R1", "1", version), ("Q:1", "R2", "1", version)],
            road_situation_interval=[("R1", "1", version, "Accident", at(-60), at(60)),
                                     ("R2", "1", version, "MaintenanceWorks", at(-60), at(240))],
            quay_road_section=[("Q:1", "S1")],
            road_travel_time=[("S1", at(0) + timedelta(milliseconds=10), True, 1.6, [])])  # as NPRA sends it
        frames = self.frames(calls)
        conditions = G.build_conditions(frames["stop_call"], roads, DAY).collect()
        got = {(r.hour, r.precipitation, r.road_surface, r.road_situation, r.traffic): r.scored_departures
               for r in conditions}
        self.assertEqual(got, {(9, "light", "freezing", "incident", "congested"): 1,
                               (9, "unknown", "unknown", "none", "no_section"): 1,
                               (0, "unknown", "unknown", "unknown", "unknown"): 1,
                               (10, "unknown", "unknown", "roadworks", "unmeasured"): 1})
        stops = G.build_stops(frames["stop_call"], *self.hubs(frames)).collect()
        self.assertEqual(sum(r.scored_departures for r in stops), sum(got.values()))
        self.assertEqual(sum(r.late_departures for r in stops), sum(r.late_departures for r in conditions))
        busy = next(r for r in stops if (r.hub_id, r.hour) == ("SP:1", 9))
        self.assertEqual((busy.hub_name, busy.scored_departures, busy.departures_3_5_min, busy.cancelled_departures),
                         ("Stop", 1, 1, 1))

    def test_partial_gate_publication_and_rerun(self):
        with tempfile.TemporaryDirectory(prefix="gold-test-") as temp:
            root = Path(temp)
            self.write_silver(root, self.frames(), self.roads(), status="partial", ready=False)
            with self.assertRaisesRegex(ValueError, "Silver is partial"):
                G.run(root, DAY, spark=self.spark)
            record = G.run(root, DAY, spark=self.spark, allow_partial=True)
            self.assertFalse(record["ready"])
            self.assertEqual(set(record["tables"]), set(G.KEYS))
            folder = root / "gold" / "line_scorecard" / f"operating_date={DAY}"
            self.assertIn("PARTIAL SAMPLE", (folder / "_preview" / "report.md").read_text())
            self.assertTrue((folder / "_preview" / "scorecard.csv").exists())
            self.assertEqual(self.spark.read.parquet(str(folder)).count(), 1)
            before = (root / "gold" / "_runs" / f"operating_date={DAY}" / "latest.json").read_text()
            self.write_silver(root, self.frames([call("j1", delay=0)]), self.roads(), rid="replacement-silver")
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
                G.source_record(root, "entur", DAY, allow_partial=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
