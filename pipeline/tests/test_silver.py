"""Silver contracts, entirely offline; small XML/JSON and Parquet fixtures."""

import json
import os
import shutil
import sys
import tempfile
import unittest
import zipfile
from datetime import date, datetime, timedelta, timezone
from importlib.util import find_spec
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import silver_parse as P
import silver_reference as R

DAY = date(2026, 10, 26)
JID = "T:ServiceJourney:1"
UTC = timezone.utc


def instant(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def base(received="2026-10-26T08:00:00Z"):
    return dict(received_at=instant(received), source_file="/landing/hour=08.jsonl.gz",
                payload_sha256=P.digest(received), bronze_run_id="fixture", source_date=str(DAY), dq_issue=[])


def call(order=1, actual=None, expected="2026-10-26T08:12:00Z", aimed="2026-10-26T08:10:00Z"):
    kind = "RecordedCall" if actual else "EstimatedCall"
    return f"""<{kind}><Order>{order}</Order><StopPointRef>NSR:Quay:1</StopPointRef>
      <AimedDepartureTime>{aimed}</AimedDepartureTime>
      {f'<ExpectedDepartureTime>{expected}</ExpectedDepartureTime>' if expected else ''}
      {f'<ActualDepartureTime>{actual}</ActualDepartureTime>' if actual else ''}</{kind}>"""


def et(calls="", recorded="2026-10-26T08:00:00Z", jid=JID, framed=True, extra=""):
    identity = f"<FramedVehicleJourneyRef><DataFrameRef>{DAY}</DataFrameRef><DatedVehicleJourneyRef>{jid}</DatedVehicleJourneyRef></FramedVehicleJourneyRef>" if framed else f"<DatedVehicleJourneyRef>{jid}</DatedVehicleJourneyRef>"
    return f"""<Siri xmlns="http://www.siri.org.uk/siri"><ServiceDelivery><EstimatedTimetableDelivery>
      <EstimatedJourneyVersionFrame><EstimatedVehicleJourney><RecordedAtTime>{recorded}</RecordedAtTime>
      {identity}<LineRef>T:Line:1</LineRef><DataSource>T</DataSource>{extra}
      <EstimatedCalls>{calls}</EstimatedCalls></EstimatedVehicleJourney></EstimatedJourneyVersionFrame>
      </EstimatedTimetableDelivery></ServiceDelivery></Siri>"""


def plan_xml():
    # Journeys deliberately precede the stop/pattern/calendar references (FLT).
    return f"""<PublicationDelivery xmlns="http://www.netex.org.uk/netex"><dataObjects>
      <ServiceJourney id="{JID}" version="1"><LineRef ref="T:Line:1"/>
        <JourneyPatternRef ref="T:JourneyPattern:1"/><dayTypes><DayTypeRef ref="T:DayType:1"/></dayTypes>
        <passingTimes><TimetabledPassingTime><StopPointInJourneyPatternRef ref="p1"/><DepartureTime>09:10:00</DepartureTime></TimetabledPassingTime>
        <TimetabledPassingTime><StopPointInJourneyPatternRef ref="p2"/><ArrivalTime>01:15:00</ArrivalTime><ArrivalDayOffset>1</ArrivalDayOffset></TimetabledPassingTime></passingTimes>
      </ServiceJourney>
      <ServiceJourney id="T:ServiceJourney:2" version="1"><LineRef ref="T:Line:1"/>
        <JourneyPatternRef ref="T:JourneyPattern:1"/><dayTypes><DayTypeRef ref="T:DayType:1"/></dayTypes>
        <passingTimes><TimetabledPassingTime><StopPointInJourneyPatternRef ref="p1"/><DepartureTime>10:10:00</DepartureTime></TimetabledPassingTime></passingTimes>
      </ServiceJourney>
      <DatedServiceJourney id="T:DatedServiceJourney:1"><ServiceJourneyRef ref="{JID}"/><OperatingDayRef ref="day1"/></DatedServiceJourney>
      <OperatingDay id="day1"><CalendarDate>{DAY}</CalendarDate></OperatingDay>
      <Line id="T:Line:1" version="1"><Name>Test line</Name><TransportMode>bus</TransportMode><OperatorRef ref="T:Operator:1"/></Line>
      <Operator id="T:Operator:1" version="1"><Name>Test operator</Name></Operator>
      <JourneyPattern id="T:JourneyPattern:1"><pointsInSequence>
        <StopPointInJourneyPattern id="p1" order="1"><ScheduledStopPointRef ref="s1"/></StopPointInJourneyPattern>
        <StopPointInJourneyPattern id="p2" order="2"><ScheduledStopPointRef ref="s1"/></StopPointInJourneyPattern>
      </pointsInSequence></JourneyPattern>
      <PassengerStopAssignment id="a1"><ScheduledStopPointRef ref="s1"/><QuayRef ref="NSR:Quay:1"/></PassengerStopAssignment>
      <DayType id="T:DayType:1"><properties><PropertyOfDay><DaysOfWeek>Monday Tuesday Wednesday Thursday Friday</DaysOfWeek></PropertyOfDay></properties></DayType>
      <OperatingPeriod id="period"><FromDate>2026-10-01T00:00:00</FromDate><ToDate>2026-10-31T00:00:00</ToDate></OperatingPeriod>
      <DayTypeAssignment id="assign"><DayTypeRef ref="T:DayType:1"/><OperatingPeriodRef ref="period"/></DayTypeAssignment>
    </dataObjects></PublicationDelivery>"""


NSR = """<PublicationDelivery xmlns="http://www.netex.org.uk/netex"><StopPlace id="NSR:StopPlace:1" version="1">
  <Name>Test stop</Name><Centroid><Location><Latitude>59</Latitude><Longitude>10</Longitude></Location></Centroid>
  <quays><Quay id="NSR:Quay:1" version="1"><Name>Platform one</Name>
    <Centroid><Location><Latitude>59.01</Latitude><Longitude>10.01</Longitude></Location></Centroid>
  </Quay></quays></StopPlace></PublicationDelivery>"""


class ParseTest(unittest.TestCase):
    def test_one_mode_vocabulary_without_guessing_direction_codes(self):
        self.assertEqual(P.canonical_mode("FERRY"), "water")
        self.assertEqual(P.canonical_mode("water"), "water")
        msg = json.dumps({"type": "next", "payload": {"data": {"vehicles": [{
            "vehicleId": "1", "direction": "Inbound", "mode": "FERRY"}]}}})
        vehicle = next(P.parse_vehicles(msg, base()))
        self.assertEqual(vehicle["direction_name"], "Inbound")
        self.assertNotIn("direction_id", vehicle)

    def test_times_are_distinct_utc_and_day_is_explicit(self):
        rows = list(P.parse_et(et(call(actual="2026-10-26T08:11:00Z")), base()))
        self.assertEqual([r["kind"] for r in rows], ["journey", "call"])
        row = rows[1]
        self.assertEqual(row["operating_date"], DAY)
        self.assertEqual(row["actual_departure_at"], instant("2026-10-26T08:11:00Z"))
        self.assertEqual(row["expected_departure_at"], instant("2026-10-26T08:12:00Z"))
        self.assertEqual(row["dq_issue"], [])

    def test_bad_offset_not_assumed_and_missing_day_not_inferred(self):
        row = list(P.parse_et(et(call(expected="2026-10-26T09:10:00"), framed=False), base()))[1]
        self.assertIsNone(row["operating_date"])
        self.assertIsNone(row["expected_departure_at"])
        self.assertIn("invalid_expected_departure_at", row["dq_issue"])

    def test_cancellation_without_calls_is_a_journey(self):
        rows = list(P.parse_et(et(extra="<Cancellation>true</Cancellation>"), base()))
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["journey_cancelled"])

    def test_protocol_and_unfamiliar_shape(self):
        self.assertEqual(list(P.parse_et("<Siri><HeartbeatNotification/></Siri>", base()))[0]["kind"], "control")
        row = dict(received_at="2026-10-26T08:00:00Z", payload="<broken", _bronze_status="ok",
                   _bronze_source_file="fixture", _bronze_payload_sha256="hash", _bronze_run_id="fixture", source_date=str(DAY))
        self.assertIn("malformed_payload", list(P.decode_partition([row], "et"))[0]["dq_issue"])
        self.assertIn("unknown_message_shape", list(P.parse_et("<Unknown/>", base()))[0]["dq_issue"])
        self.assertIn("unknown_message_shape", next(P.parse_et(
            "<Siri><EstimatedTimetableDelivery><NewJourneyShape/></EstimatedTimetableDelivery></Siri>", base()))["dq_issue"])

    def test_graphql_error_array_is_quarantined(self):
        payload = json.dumps({"type": "error", "payload": [{"message": "subscription failed"}]})
        source = dict(received_at="2026-10-26T08:00:00Z", payload=payload, _bronze_status="ok",
                      _bronze_source_file="vehicles/hour=08.jsonl.gz", _bronze_payload_sha256=P.digest(payload),
                      _bronze_run_id="fixture", source_date=str(DAY))
        rows = list(P.decode_partition([source], "vehicles"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "quarantine")
        self.assertEqual(rows[0]["dq_issue"], ["graphql_error"])
        self.assertEqual(rows[0]["payload_sha256"], source["_bronze_payload_sha256"])
        self.assertEqual(rows[0]["source_file"], source["_bronze_source_file"])

    def test_graphql_unexpected_envelope_shapes_are_quarantined(self):
        for body in ([{"message": "unexpected array"}], {"data": [{"vehicles": []}]}):
            with self.subTest(payload=body):
                rows = list(P.parse_vehicles(json.dumps({"type": "next", "payload": body}), base()))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["kind"], "quarantine")
                self.assertEqual(rows[0]["dq_issue"], ["unknown_message_shape"])

    def test_sx_versions_without_native_version_and_scopes(self):
        xml = """<Siri><PtSituationElement><ParticipantRef>T</ParticipantRef><SituationNumber>1</SituationNumber>
          <Summary xml:lang="NO">Forsinket</Summary><Summary xml:lang="EN">Delayed</Summary>
          <ValidityPeriod><StartTime>2026-10-26T09:00:00+01:00</StartTime></ValidityPeriod>
          <Affects><Lines><AffectedLine><LineRef>T:Line:1</LineRef></AffectedLine></Lines></Affects>
          </PtSituationElement></Siri>"""
        row = list(P.parse_sx(xml, base()))[0]
        changed = list(P.parse_sx(xml.replace("Forsinket", "Avlyst"), base()))[0]
        self.assertNotEqual(row["version_key"], changed["version_key"])
        self.assertEqual(len(row["summaries"]), 2)
        self.assertEqual(row["affects"], [{"entity_type": "LineRef", "entity_id": "T:Line:1"}])
        self.assertEqual(row["validity_periods"][0]["start_at"], instant("2026-10-26T08:00:00Z"))


class ReferenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="silver-reference-")
        self.root = Path(self.tmp.name)
        self.archive = self.root / "plan.zip"
        with zipfile.ZipFile(self.archive, "w") as z:
            z.writestr("T_line.xml", plan_xml())

    def tearDown(self):
        self.tmp.cleanup()

    def test_two_pass_repeated_quay_overnight_and_dated_ids(self):
        rows = list(R.plan_member(str(self.archive), "T_line.xml", None, DAY))
        calls = [r for r in rows if r["kind"] == "call" and r["journey_id"] == JID]
        self.assertEqual([r["call_order"] for r in calls], [1, 2])
        self.assertEqual({r["quay_id"] for r in calls}, {"NSR:Quay:1"})
        self.assertEqual(calls[1]["planned_arrival_at"], instant("2026-10-27T00:15:00Z"))
        journey = next(r for r in rows if r["kind"] == "journey" and r["journey_id"] == JID)
        self.assertEqual(journey["dated_journey_ids"], ["T:DatedServiceJourney:1"])

    def test_calendar_date_exclusion_overrides_period(self):
        for false in ("false", "0"):
            with self.subTest(availability=false):
                xml = plan_xml().replace("</dataObjects>", f"""<DayTypeAssignment id="exclude"><DayTypeRef ref="T:DayType:1"/>
                  <Date>{DAY}</Date><isAvailable>{false}</isAvailable></DayTypeAssignment></dataObjects>""")
                with zipfile.ZipFile(self.archive, "w") as z:
                    z.writestr("T_line.xml", xml)
                journeys = [r["journey_id"] for r in R.plan_member(str(self.archive), "T_line.xml", None, DAY) if r["kind"] == "journey"]
                # The explicit dated service still runs; the calendar-only one does not.
                self.assertEqual(journeys, [JID])

    def test_repeated_assignment_id_retains_both_day_types(self):
        xml = plan_xml().replace("</dataObjects>", f"""<DayType id="T:DayType:2"/>
          <DayTypeAssignment id="assign"><DayTypeRef ref="T:DayType:2"/><Date>{DAY}</Date></DayTypeAssignment>
          </dataObjects>""")
        with zipfile.ZipFile(self.archive, "w") as z:
            z.writestr("T_line.xml", xml)
        index = R.index_member(str(self.archive), "T_line.xml", DAY)
        calendar = R.Calendar([index], DAY)
        self.assertTrue(calendar.active("T:DayType:1"))
        self.assertTrue(calendar.active("T:DayType:2"))

    def test_flexible_scope_uses_structure_and_preserves_time_window(self):
        xml = plan_xml().replace("<Line ", "<FlexibleLine ").replace("</Line>", "</FlexibleLine>")
        xml = xml.replace("<DepartureTime>09:10:00</DepartureTime>", "<EarliestDepartureTime>09:10:00</EarliestDepartureTime>")
        # Deliberately put the calendar in the flexible shared file and give the
        # line an opaque filename, like the real SKY flexible members.
        import xml.etree.ElementTree as ET
        root = R.strip_ns(ET.fromstring(xml))
        shared = ET.Element("PublicationDelivery")
        objects = root.find("dataObjects")
        for elem in list(objects):
            if elem.tag in {"DayType", "OperatingPeriod", "DayTypeAssignment", "OperatingDay"}:
                objects.remove(elem)
                shared.append(elem)
        with zipfile.ZipFile(self.archive, "w") as z:
            z.writestr("T_places.xml", ET.tostring(root))
            z.writestr("_T_flexible_shared_data.xml", ET.tostring(shared))
        rows = list(R.plan_member(str(self.archive), "T_places.xml", None, DAY))
        call_row = next(r for r in rows if r["kind"] == "call" and r["call_order"] == 1)
        self.assertTrue(call_row["is_flexible"])
        self.assertIsNone(call_row["planned_departure_at"])
        self.assertEqual(call_row["earliest_departure_at"], instant("2026-10-26T08:10:00Z"))

    def test_fallback_calendar_keeps_its_own_period_scope(self):
        own = {k: {} for k in R.INDEX_TAGS}
        primary = {k: {} for k in R.INDEX_TAGS}
        secondary = {k: {} for k in R.INDEX_TAGS}
        primary["OperatingPeriod"]["1"] = {"start": "2030-01-01", "end": "2030-12-31"}
        secondary["DayType"]["flex"] = {"weekdays": ["Monday"], "unsupported": False}
        secondary["OperatingPeriod"]["1"] = {"start": "2026-10-01", "end": "2026-10-31"}
        secondary["DayTypeAssignment"]["a"] = {"day_type": "flex", "date": None, "day_ref": None,
                                                 "period": "1", "available": True}
        fallback = R.Calendar([own, secondary], DAY)
        self.assertTrue(R.Calendar([own, primary], DAY, [fallback]).active("flex"))

    def test_archive_never_uses_future_or_partial(self):
        for d in (DAY - timedelta(days=1), DAY + timedelta(days=1)):
            folder = self.root / "netex" / f"date={d}"
            folder.mkdir(parents=True)
            shutil.copyfile(self.archive, folder / "plan.zip")
        chosen, _ = R.archived_file(self.root, "netex", DAY)
        self.assertEqual(chosen, DAY - timedelta(days=1))
        with self.assertRaises(FileNotFoundError):
            R.archived_file(self.root, "netex", DAY - timedelta(days=2))

    def test_dst_times_are_flagged_instead_of_guessed(self):
        issues = []
        self.assertIsNone(R.planned_time("02:30:00", 0, date(2026, 10, 25), issues, "time"))
        self.assertEqual(issues, ["ambiguous_time"])
        issues = []
        self.assertIsNone(R.planned_time("02:30:00", 0, date(2026, 3, 29), issues, "time"))
        self.assertEqual(issues, ["nonexistent_time"])
        self.assertEqual(R.planned_time("02:30:00+01:00", 0, date(2026, 10, 25), [], "time"), instant("2026-10-25T01:30:00Z"))


@unittest.skipUnless(find_spec("pyspark"), "pyspark not installed")
class SparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import silver
        from pyspark.sql import SparkSession
        cls.S = silver
        cls.spark = SparkSession.builder.master("local[2]").appName("silver-tests") \
            .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2").config("spark.sql.parquet.compression.codec", "zstd") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.sparkContext.addPyFile(str(Path(R.__file__)))
        cls.spark.sparkContext.addPyFile(str(Path(P.__file__)))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def versions(self, entries):
        rows = []
        for received, recorded, xml in entries:
            rows.extend(P.parse_et(et(xml, recorded=recorded), base(received)))
        return self.spark.createDataFrame(rows, P.schema("et"))

    def target_calls(self):
        return self.spark.createDataFrame([dict(operating_date=DAY, journey_id=JID, call_order=1,
            planned_arrival_at=None, aimed_arrival_at=None, actual_arrival_at=None,
            planned_departure_at=instant("2026-10-26T08:10:00Z"), aimed_departure_at=None,
            actual_departure_at=instant("2026-10-26T08:11:00Z"))],
            "operating_date date, journey_id string, call_order int, planned_arrival_at timestamp, aimed_arrival_at timestamp, actual_arrival_at timestamp, planned_departure_at timestamp, aimed_departure_at timestamp, actual_departure_at timestamp")

    def test_duplicate_reference_keys_fail_before_joining(self):
        with self.assertRaisesRegex(ValueError, "duplicate primary key"):
            self.S.assert_unique(self.spark.createDataFrame([("1",), ("1",)], ["id"]), ["id"], "quays")

    def test_reference_versions_sort_numerically_and_keep_conflicting_ties(self):
        refs = self.spark.createDataFrame([("1", "9", "old"), ("1", "10", "current")], ["id", "version", "name"])
        self.assertEqual(self.S.current_reference(refs).first()["name"], "current")
        conflict = self.spark.createDataFrame([("1", "10", "a"), ("1", "10", "b")], ["id", "version", "name"])
        with self.assertRaisesRegex(ValueError, "duplicate primary key"):
            self.S.assert_unique(self.S.current_reference(conflict), ["id"], "operators")

    def test_vehicle_id_is_scoped_by_codespace(self):
        records = []
        for codespace in ("T", "T", "OTHER"):
            msg = json.dumps({"type": "next", "payload": {"data": {"vehicles": [{
                "vehicleId": "123", "codespace": {"codespaceId": codespace}, "lastUpdated": "2026-10-26T08:00:00Z"}]}}})
            records.extend(P.parse_vehicles(msg, base()))
        df = self.spark.createDataFrame(records, P.schema("vehicles"))
        self.assertEqual(self.S.latest(df, ["codespace", "vehicle_id", "last_updated_at"]).count(), 2)

    def test_explicit_dated_reference_resolves_vehicle_date(self):
        alias = "T:DatedServiceJourney:1"
        aliases = self.spark.createDataFrame([(alias, JID, DAY)],
            "dated_journey_id string, journey_id string, operating_date date")
        cases = [
            ("vehicle", JID, alias, None),
            ("et_alias", alias, None, None),
            ("reported_date", JID, alias, DAY + timedelta(days=1)),
            ("unmapped", JID, "T:DatedServiceJourney:unknown", None),
            ("conflicting_id", "T:ServiceJourney:other", alias, None),
        ]
        versions = self.spark.createDataFrame([
            dict(base(), kind="vehicle", vehicle_id=case, journey_id=jid, dated_journey_id=dated, operating_date=op)
            for case, jid, dated, op in cases], P.schema("vehicles"))
        rows = {r["vehicle_id"]: r for r in self.S.normalize_journeys(versions, aliases).collect()}
        self.assertEqual(len(rows), len(cases))
        for case in ("vehicle", "et_alias"):
            self.assertEqual(rows[case]["operating_date"], DAY)
            self.assertEqual(rows[case]["journey_id"], JID)
            self.assertEqual(rows[case]["dq_issue"], [])
        self.assertEqual(rows["vehicle"]["source_journey_id"], JID)
        self.assertEqual(rows["et_alias"]["source_journey_id"], alias)
        self.assertEqual(rows["reported_date"]["operating_date"], DAY + timedelta(days=1))
        self.assertEqual(rows["reported_date"]["dq_issue"], ["conflicting_operating_date"])
        self.assertIsNone(rows["unmapped"]["operating_date"])
        self.assertEqual(rows["unmapped"]["journey_id"], JID)
        self.assertEqual(rows["conflicting_id"]["source_journey_id"], "T:ServiceJourney:other")
        self.assertEqual(rows["conflicting_id"]["dq_issue"], ["conflicting_journey_id"])

    def test_cancellation_only_journey_is_visible_at_prediction_cutoff(self):
        records = list(P.parse_et(et(call()), base("2026-10-26T08:00:01Z")))
        records += list(P.parse_et(et(recorded="2026-10-26T08:03:00Z", extra="<Cancellation>true</Cancellation>"), base("2026-10-26T08:03:01Z")))
        records += list(P.parse_et(et(recorded="2026-10-26T07:58:00Z", extra="<Cancellation>false</Cancellation>"), base("2026-10-26T08:04:00Z")))
        versions = self.spark.createDataFrame(records, P.schema("et"))
        rows = self.S.build_predictions(versions, self.target_calls(), leads=[5]).collect()
        self.assertTrue(rows)
        self.assertTrue(all(r["journey_cancelled"] for r in rows))
        self.assertTrue(all(r["journey_received_at"] <= r["cutoff_at"] for r in rows))

    def test_cancellation_samples_do_not_require_a_preceding_call(self):
        from pyspark.sql import functions as F
        target = self.target_calls().withColumn("actual_departure_at", F.lit(None).cast("timestamp"))
        for case in ("no_calls", "first_call_after_cutoff", "cancellation_after_cutoff"):
            with self.subTest(case=case):
                received = "2026-10-26T08:05:01Z" if case == "cancellation_after_cutoff" else "2026-10-26T08:03:01Z"
                cancelled = base(received)
                cancelled["dq_issue"] = ["journey_source_warning"]
                records = list(P.parse_et(et(recorded="2026-10-26T08:03:00Z", extra="<Cancellation>true</Cancellation>"), cancelled))
                if case == "first_call_after_cutoff":
                    records += list(P.parse_et(et(call(), recorded="2026-10-26T08:06:00Z"), base("2026-10-26T08:06:01Z")))
                versions = self.spark.createDataFrame(records, P.schema("et"))
                rows = self.S.build_predictions(versions, target, leads=[5]).collect()
                if case == "cancellation_after_cutoff":
                    self.assertEqual(rows, [])
                    continue
                self.assertEqual(len(rows), 1)
                row = rows[0]
                self.assertEqual((row["event"], row["reference"]), ("departure", "planned"))
                self.assertTrue(row["journey_cancelled"])
                self.assertIsNone(row["predicted_at"])
                self.assertIsNone(row["call_cancelled"])
                self.assertEqual(row["payload_sha256"], cancelled["payload_sha256"])
                self.assertEqual(row["source_file"], cancelled["source_file"])
                self.assertEqual(row["bronze_run_id"], cancelled["bronze_run_id"])
                self.assertEqual(row["dq_issue"], ["journey_source_warning"])
                self.assertLessEqual(row["recorded_at"], row["cutoff_at"])
                self.assertLessEqual(row["received_at"], row["cutoff_at"])
                self.assertEqual(row["available_at"], row["journey_received_at"])

    def test_actual_wins_replay_and_latest_journey_is_independent(self):
        v = self.versions([
            ("2026-10-26T08:11:05Z", "2026-10-26T08:11:00Z", call(actual="2026-10-26T08:11:00Z")),
            ("2026-10-26T08:12:00Z", "2026-10-26T08:12:00Z", call()),
            ("2026-10-26T08:13:00Z", "2026-10-26T08:10:00Z", call(expected="2026-10-26T08:20:00Z"))])
        from pyspark.sql import functions as F
        winner = self.S.latest(v.filter(F.col("kind") == "call"), self.S.CALL_KEY, actual_first=True).first()
        self.assertEqual(winner["actual_departure_at"].timestamp(), instant("2026-10-26T08:11:00Z").timestamp())
        journey = self.S.latest(v.filter(F.col("kind") == "journey"), self.S.JOURNEY_KEY).first()
        self.assertEqual(journey["recorded_at"].timestamp(), instant("2026-10-26T08:12:00Z").timestamp())

    def test_plan_enrichment_does_not_fabricate_a_reported_quay(self):
        versions = self.versions([("2026-10-26T08:00:01Z", "2026-10-26T08:00:00Z",
                                  call().replace("<StopPointRef>NSR:Quay:1</StopPointRef>", ""))])
        plan = self.spark.createDataFrame([(DAY, JID, 1, "NSR:Quay:1", [], "fixture")],
            "operating_date date, journey_id string, call_order int, quay_id string, dq_issue array<string>, source_member string")
        journeys = self.spark.createDataFrame([(DAY, JID, None, None, True)],
            "operating_date date, journey_id string, journey_cancelled boolean, extra_journey boolean, has_realtime boolean")
        result = self.S.build_calls(versions, plan, journeys).first()
        self.assertEqual(result["quay_id"], "NSR:Quay:1")
        self.assertIsNone(result["reported_quay_id"])
        self.assertIn("missing_reported_quay", result["dq_issue"])

    def test_predictions_exclude_late_received_and_future_producer(self):
        v = self.versions([
            ("2026-10-26T08:00:01Z", "2026-10-26T08:00:00Z", call(expected="2026-10-26T08:12:00Z")),
            ("2026-10-26T08:04:50Z", "2026-10-26T07:59:00Z", call(expected="2026-10-26T08:20:00Z")),
            ("2026-10-26T08:06:00Z", "2026-10-26T08:04:00Z", call(expected="2026-10-26T08:13:00Z")),
            ("2026-10-26T08:04:30Z", "2026-10-26T08:07:00Z", call(expected="2026-10-26T08:14:00Z"))])
        final = self.target_calls()
        rows = self.S.build_predictions(v, final, leads=[5]).collect()
        planned = next(r for r in rows if r["reference"] == "planned")
        actual = next(r for r in rows if r["reference"] == "actual")
        self.assertEqual(planned["predicted_at"].timestamp(), instant("2026-10-26T08:12:00Z").timestamp())
        self.assertEqual(actual["predicted_at"].timestamp(), instant("2026-10-26T08:13:00Z").timestamp())
        self.assertTrue(all(r["received_at"] <= r["cutoff_at"] and r["recorded_at"] <= r["cutoff_at"] for r in rows))

    def test_unusable_bronze_blocks_readiness_and_preserves_published_day(self):
        with tempfile.TemporaryDirectory(prefix="silver-contract-gate-") as tmp:
            root = Path(tmp)
            landing, output = root / "landing", root / "out"
            for dataset, xml in (("netex", plan_xml()), ("nsr", NSR)):
                folder = landing / dataset / f"date={DAY}"
                folder.mkdir(parents=True)
                with zipfile.ZipFile(folder / "fixture.zip", "w") as z:
                    z.writestr("T_line.xml", xml)
            cases = [("et", DAY - timedelta(days=1), "corrupt"), ("et", DAY, "incomplete"),
                     ("et", DAY + timedelta(days=1), "wrong_endpoint"), ("sx", DAY, "corrupt"),
                     ("vehicles", DAY, "ok")]
            for feed, source_day, status in cases:
                payload = json.dumps({"type": "next", "payload": {"data": {"vehicles": [None]}}}) if feed == "vehicles" else None
                rows = [(f"{source_day}T{h:02d}:00:01Z", payload, status, f"{h:02d}",
                         f"fixture/{feed}/{source_day}/{h:02d}", P.digest(payload) if payload else None,
                         "fixture", f"broken envelope {h}" if status == "corrupt" else None) for h in range(24)]
                self.spark.createDataFrame(rows,
                    "received_at string, payload string, _bronze_status string, hour string, _bronze_source_file string, _bronze_payload_sha256 string, _bronze_run_id string, _bronze_raw_line string") \
                    .write.parquet(str(output / "bronze" / self.S.DATASETS[feed] / f"date={source_day}"))
            marker = output / "silver" / "_runs" / f"operating_date={DAY}" / "latest.json"
            previous = {"run_id": "previous", "status": "complete", "ready": True}
            self.S.atomic_json(marker, previous)
            expected = {"bronze_corrupt": 48, "bronze_incomplete": 24, "bronze_wrong_endpoint": 24,
                        "invalid_vehicle_shape": 24}
            with self.assertRaisesRegex(ValueError, "source contract errors"):
                self.S.run(landing, output, DAY, spark=self.spark, leads=[5])
            self.assertEqual(json.loads(marker.read_text()), previous)
            history = next(p for p in marker.parent.glob("*.json") if p.name != "latest.json")
            failed = json.loads(history.read_text())
            self.assertEqual(failed["status"], "failed")
            self.assertFalse(failed["ready"])
            self.assertEqual(failed["contract_errors"], expected)
            self.assertEqual(failed["tables"]["quarantine"]["rows"], 120)
            self.assertFalse((output / "silver" / "stop_call" / f"operating_date={DAY}").exists())
            partial = self.S.run(landing, output, DAY, spark=self.spark, allow_partial=True, leads=[5])
            self.assertEqual(partial["status"], "partial")
            self.assertFalse(partial["ready"])
            self.assertEqual(partial["contract_errors"], expected)
            self.assertEqual(partial["tables"]["quarantine"]["rows"], 120)
            self.assertFalse(json.loads(marker.read_text())["ready"])
            for feed, source_day, _ in cases:
                self.assertEqual(partial["inputs"][feed][str(source_day)]["missing_hours"], [])
            self.assertFalse(any((output / "silver" / "_staging").iterdir()))

    def test_full_pipeline_denominator_quarantine_and_rerun(self):
        from pyspark.sql import functions as F
        with tempfile.TemporaryDirectory(prefix="silver-e2e-") as tmp:
            root = Path(tmp)
            landing, output = root / "landing", root / "out"
            for dataset, filename, xml in (("netex", "plan.zip", plan_xml()), ("nsr", "stops.zip", NSR)):
                folder = landing / dataset / f"date={DAY}"
                folder.mkdir(parents=True)
                with zipfile.ZipFile(folder / filename, "w") as z:
                    z.writestr("T_line.xml", xml)
            def bronze(feed, source_day, payloads):
                source = output / "bronze" / self.S.DATASETS[feed] / f"date={source_day}"
                records = [(f"{source_day}T{h:02d}:00:01Z", p, "ok", f"{h:02d}", "source", P.digest(p), "fixture")
                           for h in range(24) for p in (payloads if h == 8 else ["<Siri><HeartbeatNotification/></Siri>" if feed != "vehicles" else '{"type":"connection_ack"}'])]
                self.spark.createDataFrame(records, "received_at string, payload string, _bronze_status string, hour string, _bronze_source_file string, _bronze_payload_sha256 string, _bronze_run_id string").write.mode("overwrite").parquet(str(source))
            for offset in (-1, 0, 1):
                payloads = [et(call() + call(order=2, actual="2026-10-27T00:16:00Z", aimed="2026-10-27T00:15:00Z") + call().replace("<Order>1</Order>", ""),
                               recorded="2026-10-27T00:16:01Z", jid="T:DatedServiceJourney:1", framed=False)] if offset == 1 else [et(call())]
                bronze("et", DAY + timedelta(days=offset), payloads)
            bronze("sx", DAY, ["<Siri><PtSituationElement><ParticipantRef>T</ParticipantRef><SituationNumber>1</SituationNumber><Summary>Delay</Summary></PtSituationElement></Siri>"])
            vehicle = {"vehicleId": "1", "codespace": {"codespaceId": "T"}, "lastUpdated": "2026-10-26T08:00:00Z",
                       "serviceJourney": {"id": JID, "date": str(DAY)}, "location": {"latitude": 95, "longitude": 10}}
            bronze("vehicles", DAY, [json.dumps({"type": "next", "payload": {"data": {"vehicles": [vehicle, vehicle]}}})])
            before = {p: p.read_bytes() for p in landing.rglob("*.zip")}
            record = self.S.run(landing, output, DAY, spark=self.spark, leads=[5])
            self.assertTrue(record["ready"])
            self.assertEqual(record["call_coverage"], {"matched": 2, "no_realtime": 1})
            def read(table, partition="operating_date"):
                return self.spark.read.parquet(str(output / "silver" / table / f"{partition}={DAY}"))
            calls = read("stop_call")
            self.assertEqual(calls.count(), 3)
            no_rt = calls.filter(~F.col("has_realtime")).first()
            self.assertIsNone(no_rt["actual_departure_at"])
            self.assertEqual(read("vehicle_position", "observation_date").count(), 1)
            self.assertIn("invalid_coordinates", read("vehicle_position", "observation_date").first()["dq_issue"])
            self.assertEqual(read("quarantine").count(), 1)
            self.assertIn("missing_call_order", read("quarantine").first()["dq_issue"])
            self.assertEqual(before, {p: p.read_bytes() for p in landing.rglob("*.zip")})
            marker = output / "silver" / "_runs" / f"operating_date={DAY}" / "latest.json"
            previous = marker.read_bytes()
            # A missing input on a rerun must leave the previously published day intact.
            shutil.rmtree(output / "bronze" / "siri-et" / f"date={DAY - timedelta(days=1)}")
            with self.assertRaises(ValueError):
                self.S.run(landing, output, DAY, spark=self.spark)
            self.assertEqual(marker.read_bytes(), previous)
            self.assertEqual(read("stop_call").count(), 3)
            other = output / "silver" / "stop_call" / f"operating_date={DAY - timedelta(days=1)}" / "kept.txt"
            other.parent.mkdir(parents=True)
            other.write_text("untouched")
            # Rebuild with no ET reports: old actuals and old predictions must
            # disappear, while other days and the immutable references survive.
            for offset in (-1, 0, 1):
                bronze("et", DAY + timedelta(days=offset), ["<Siri><HeartbeatNotification/></Siri>"])
            again = self.S.run(landing, output, DAY, spark=self.spark, leads=[5])
            self.assertTrue(again["ready"])
            self.assertEqual(again["call_coverage"], {"no_realtime": 3})
            self.assertEqual(read("call_prediction").count(), 0)
            self.assertEqual(read("stop_call").filter(F.col("actual_departure_at").isNotNull()).count(), 0)
            self.assertEqual(other.read_text(), "untouched")
            self.assertEqual(before, {p: p.read_bytes() for p in landing.rglob("*.zip")})


class PublishTest(unittest.TestCase):
    def test_failed_partition_install_restores_published_day(self):
        import silver
        with tempfile.TemporaryDirectory(prefix="silver-publish-") as tmp:
            root, stage = Path(tmp) / "silver", Path(tmp) / "stage"
            partition = f"operating_date={DAY}"
            tables = {table: {"partition": partition} for table in ("journey", "stop_call")}
            previous = {"run_id": "old", "ready": True, "status": "complete", "tables": tables}
            marker = root / "_runs" / partition / "latest.json"
            silver.atomic_json(marker, previous)
            for table in tables:
                for parent, content in ((root, "old"), (stage, "new")):
                    folder = parent / table / partition
                    folder.mkdir(parents=True)
                    (folder / "data.parquet").write_text(content)
            replace = os.replace
            def fail_second(source, dest):
                if source == stage / "stop_call" / partition:
                    raise OSError("simulated failed install")
                return replace(source, dest)
            with patch.object(silver.os, "replace", side_effect=fail_second):
                with self.assertRaisesRegex(OSError, "simulated"):
                    silver.publish(stage, root, dict(previous, run_id="new"), DAY)
            self.assertEqual(json.loads(marker.read_text()), previous)
            for table in tables:
                self.assertEqual((root / table / partition / "data.parquet").read_text(), "old")


if __name__ == "__main__":
    unittest.main()
