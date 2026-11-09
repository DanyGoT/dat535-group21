"""DATEX Silver contracts, entirely offline; snippets cut from real NPRA snapshots (2026-11-05)."""

import json
import shutil
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from importlib.util import find_spec
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import silver_datex_parse as D

DAY = date(2026, 11, 5)     # a Thursday, winter time
NS = 'xmlns="http://datex2.eu/schema/3/common" xmlns:ns2="http://datex2.eu/schema/3/messageContainer" ' \
     'xmlns:ns6="http://datex2.eu/schema/3/roadTrafficData" xmlns:ns9="http://datex2.eu/schema/3/locationReferencing" ' \
     'xmlns:ns12="http://datex2.eu/schema/3/situation" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'


def utc(text):
    return datetime.fromisoformat(text + "+00:00")


def container(inner, publication="2026-11-05T13:01:07.165+01:00"):
    return f'<ns2:messageContainer {NS}><ns2:payload xsi:type="x"><publicationTime>{publication}</publicationTime>{inner}</ns2:payload></ns2:messageContainer>'


def record(rid="r1", version="2", kind="MaintenanceWorks", validity=""):
    return f"""<ns12:situationRecord xsi:type="ns12:{kind}" id="{rid}" version="{version}">
      <ns12:situationRecordCreationTime>2026-06-01T08:11:05.8068938+02:00</ns12:situationRecordCreationTime>
      <ns12:situationRecordVersionTime>2026-06-09T12:52:15.6464439+02:00</ns12:situationRecordVersionTime>
      <ns12:probabilityOfOccurrence>certain</ns12:probabilityOfOccurrence><ns12:severity>low</ns12:severity>
      <ns12:validity><validityStatus>definedByValidityTimeSpec</validityStatus><validityTimeSpecification>
        <overallStartTime>2026-06-01T00:00:00+02:00</overallStartTime><overallEndTime>2026-12-24T23:59:00+01:00</overallEndTime>{validity}
      </validityTimeSpecification></ns12:validity>
      <ns12:impact><ns12:numberOfLanesRestricted>1</ns12:numberOfLanesRestricted></ns12:impact>
      <ns12:generalPublicComment><ns12:comment><values><value lang="no">Vegarbeid.|Fartsgrense 50 km/t.</value></values></ns12:comment>
        <ns12:commentType>dataProcessingNote</ns12:commentType></ns12:generalPublicComment>
      <ns12:locationReference xsi:type="ns9:LocationGroupByList">
        <ns9:locationContainedInGroup xsi:type="ns9:LinearLocation">
          <ns9:coordinatesForDisplay><ns9:latitude>59.946175</ns9:latitude><ns9:longitude>10.594271</ns9:longitude></ns9:coordinatesForDisplay>
          <ns9:supplementaryPositionalDescription><ns9:lengthAffected>1585.0</ns9:lengthAffected>
            <ns9:locationDescription><values><value lang="no">Fv. 168 Nordveien i Bærum</value></values></ns9:locationDescription>
            <ns9:namedArea xsi:type="ns9:IsoNamedArea"><ns9:areaName><values><value lang="no">Akershus</value></values></ns9:areaName><ns9:subdivisionCode>32</ns9:subdivisionCode></ns9:namedArea>
            <ns9:roadInformation><ns9:roadName>Griniveien</ns9:roadName><ns9:roadNumber>F168</ns9:roadNumber></ns9:roadInformation>
          </ns9:supplementaryPositionalDescription>
          <ns9:gmlLineString srsName="25833"><ns9:posList>254705.76 6653854.819 254664.585 6653857.558</ns9:posList></ns9:gmlLineString>
        </ns9:locationContainedInGroup>
        <ns9:locationContainedInGroup xsi:type="ns9:SingleRoadLinearLocation"><ns9:alertCLinear/></ns9:locationContainedInGroup>
      </ns12:locationReference></ns12:situationRecord>"""


def situation(*records):
    return container('<ns12:situation id="NPRA_1"><ns12:overallSeverity>low</ns12:overallSeverity>' + "".join(records) + "</ns12:situation>")


NIGHTS = """<validPeriod><startOfPeriod>2026-09-01T07:00:00+02:00</startOfPeriod><endOfPeriod>2026-11-30T20:30:00+01:00</endOfPeriod>
  <recurringTimePeriodOfDay><startTimeOfPeriod>20:00:00.0000000+01:00</startTimeOfPeriod><endTimeOfPeriod>06:00:00.0000000+01:00</endTimeOfPeriod></recurringTimePeriodOfDay>
  <recurringDayWeekMonthPeriod><applicableDay>monday</applicableDay><applicableDay>wednesday</applicableDay></recurringDayWeekMonthPeriod></validPeriod>"""

TRAVEL = container("""
  <ns6:physicalQuantity xsi:type="ns6:SinglePhysicalQuantity"><ns6:pertinentLocation xsi:type="ns9:LocationByReference"><ns9:predefinedLocationReference id="100357" version="1"/></ns6:pertinentLocation>
    <ns6:basicData xsi:type="ns6:TravelTimeData"><ns6:measurementOrCalculationTime><ns6:period><startOfPeriod>2026-11-05T03:55:00.010+01:00</startOfPeriod><endOfPeriod>2026-11-05T04:00:00.010+01:00</endOfPeriod></ns6:period></ns6:measurementOrCalculationTime>
      <ns6:freeFlowTravelTime><ns6:duration>150.0</ns6:duration></ns6:freeFlowTravelTime><ns6:freeFlowSpeed><speed>78.0</speed></ns6:freeFlowSpeed></ns6:basicData></ns6:physicalQuantity>
  <ns6:physicalQuantity xsi:type="ns6:SinglePhysicalQuantity"><ns6:pertinentLocation xsi:type="ns9:LocationByReference"><ns9:predefinedLocationReference id="100357" version="1"/></ns6:pertinentLocation>
    <ns6:basicData xsi:type="ns6:TrafficStatus"><ns6:trafficStatus><ns6:trafficStatusValue>unknown</ns6:trafficStatusValue></ns6:trafficStatus></ns6:basicData></ns6:physicalQuantity>
  <ns6:physicalQuantity xsi:type="ns6:SinglePhysicalQuantity"><ns6:pertinentLocation xsi:type="ns9:LocationByReference"><ns9:predefinedLocationReference id="100356" version="1"/></ns6:pertinentLocation>
    <ns6:basicData xsi:type="ns6:TravelTimeData"><ns6:measurementOrCalculationTime><ns6:period><startOfPeriod>2026-11-05T03:55:00.010+01:00</startOfPeriod><endOfPeriod>2026-11-05T04:00:00.010+01:00</endOfPeriod></ns6:period></ns6:measurementOrCalculationTime>
      <ns6:travelTimeTrendType>stable</ns6:travelTimeTrendType><ns6:travelTimeType>estimated</ns6:travelTimeType><ns6:travelTime><ns6:duration>119.0</ns6:duration></ns6:travelTime>
      <ns6:freeFlowTravelTime><ns6:duration>150.0</ns6:duration></ns6:freeFlowTravelTime></ns6:basicData></ns6:physicalQuantity>
  <ns6:physicalQuantity xsi:type="ns6:SinglePhysicalQuantity"><ns6:pertinentLocation xsi:type="ns9:LocationByReference"><ns9:predefinedLocationReference id="100356" version="1"/></ns6:pertinentLocation>
    <ns6:basicData xsi:type="ns6:TrafficStatus"><ns6:trafficStatus><ns6:trafficStatusValue>freeFlow</ns6:trafficStatusValue></ns6:trafficStatus></ns6:basicData></ns6:physicalQuantity>""")


def quantity(index, body):
    return f'<ns6:physicalQuantity index="{index}"><ns6:physicalQuantity xsi:type="ns6:SinglePhysicalQuantity"><ns6:basicData xsi:type="x">{body}</ns6:basicData></ns6:physicalQuantity></ns6:physicalQuantity>'


def site_measurements(site="1629006", at="2026-11-05T03:41:00.000+01:00", air=6.0, surface=5.4):
    return f"""<ns6:siteMeasurements><ns6:measurementSiteReference id="{site}" version="3564"/>
      {quantity(101, f'<ns6:temperature><airTemperature><temperature>{air}</temperature></airTemperature></ns6:temperature>')}
      {quantity(201, '<ns6:humidity><relativeHumidity><percentage>72.9</percentage></relativeHumidity></ns6:humidity>')}
      {quantity(802, '<ns6:roadSurfaceConditionMeasurements><roadSurfaceTemperature><temperature>9.9</temperature></roadSurfaceTemperature></ns6:roadSurfaceConditionMeasurements>')}
      {quantity(801, f'<ns6:roadSurfaceConditionMeasurements><roadSurfaceTemperature><temperature>{surface}</temperature></roadSurfaceTemperature></ns6:roadSurfaceConditionMeasurements>')}
      {quantity(901, '<ns6:measurementOrCalculationTime><ns6:timeValue>2026-11-05T03:40:00+01:00</ns6:timeValue></ns6:measurementOrCalculationTime><ns6:wind><windSpeed><windSpeed>2.6</windSpeed></windSpeed></ns6:wind>')}
      {quantity(1401, '<ns6:visibility><minimumVisibilityDistance><integerMetreDistance>20000</integerMetreDistance></minimumVisibilityDistance></ns6:visibility>')}
      {quantity(2501, '<ns6:precipitationDetail><precipitationIntensity><millimetresPerHourIntensity>0.4</millimetresPerHourIntensity></precipitationIntensity></ns6:precipitationDetail>')}
      {quantity(5401, '<ns6:roadSurfaceConditionMeasurements><friction><friction>0.79</friction></friction></ns6:roadSurfaceConditionMeasurements>')}
      {quantity(11201, '<weatherRelatedRoadConditionType><weatherRelatedRoadConditionType>wet</weatherRelatedRoadConditionType></weatherRelatedRoadConditionType>')}
      <ns6:measurementTimeDefault><ns6:timeValue>{at}</ns6:timeValue></ns6:measurementTimeDefault></ns6:siteMeasurements>"""


SITES = container("""<ns6:measurementSiteTable id="WOST" version="1"><ns6:measurementSite id="1629006" version="3564">
  <ns6:measurementSiteName><values><value lang="nob">Fv 714 Våvatnet</value></values></ns6:measurementSiteName>
  <ns6:measurementSiteLocation xsi:type="ns9:PointLocation"><ns9:supplementaryPositionalDescription>
    <ns9:namedArea xsi:type="ns9:IsoNamedArea"><ns9:areaName><values><value lang="nob">Trøndelag</value></values></ns9:areaName><ns9:subdivisionCode>50</ns9:subdivisionCode></ns9:namedArea>
    <ns9:roadInformation><ns9:roadNumber>F714</ns9:roadNumber></ns9:roadInformation></ns9:supplementaryPositionalDescription>
  <ns9:pointByCoordinates><ns9:pointCoordinates><ns9:latitude>63.309277</ns9:latitude><ns9:longitude>9.568446</ns9:longitude></ns9:pointCoordinates></ns9:pointByCoordinates>
  </ns6:measurementSiteLocation></ns6:measurementSite></ns6:measurementSiteTable>""")

SECTIONS = container("""<ns9:predefinedLocationReference xsi:type="ns9:PredefinedLocation" id="100357" version="1">
  <ns9:predefinedLocationName><values><value lang="no">Åsanevegen - Eikås</value></values></ns9:predefinedLocationName>
  <ns9:location xsi:type="ns9:LinearLocation"><ns9:gmlLineString srsName="http://www.opengis.net/gml/srs/epsg.xml#32633"><ns9:posList>-30621 6742669 -30603 6742707</ns9:posList></ns9:gmlLineString></ns9:location>
  </ns9:predefinedLocationReference>""")


def bronze_row(payload, feed="situation", status="ok", received="2026-11-05T12:00:00.000Z", digest=None):
    return dict(received_at=received, payload=payload, _bronze_source_file="/landing/hour=12.jsonl.gz",
                _bronze_payload_sha256=digest or str(hash(payload)), _bronze_run_id="fixture", _bronze_status=status, source_date=str(DAY))


class GeometryTest(unittest.TestCase):
    def test_utm33_matches_pyproj(self):
        # Expected values from pyproj (EPSG:25833 and 32633 agree to the printed digits), including
        # a negative easting north of Bergen and the far north-east.
        for east, north, lat, lon in ((254705.76, 6653854.819, 59.948799, 10.606923),
                                      (-30621, 6742669, 60.468208, 5.326411),
                                      (317162.291, 7138145.059, 64.320259, 11.216861),
                                      (1000000, 7800000, 69.823213, 28.075684)):
            self.assertEqual(D.utm33_to_wgs84(east, north), (lat, lon))

    def test_points_along_a_line_fill_long_segments_and_thin_dense_ones(self):
        tunnel = [dict(latitude=60.0, longitude=10.0), dict(latitude=60.0, longitude=10.02)]    # ~1.1 km
        points = D.along_path(tunnel)
        self.assertEqual(points[0], tunnel[0])
        self.assertEqual(points[-1], tunnel[-1])
        self.assertTrue(all(D.distance_m(a, b) < 200 for a, b in zip(points, points[1:])), points)
        bend = [dict(latitude=60.0, longitude=10.0 + i * 0.00002) for i in range(100)]    # 1 m apart
        self.assertLess(len(D.along_path(bend)), 5)
        self.assertEqual(D.along_path([]), [])

    def test_odd_coordinate_count_is_flagged(self):
        issues = []
        self.assertIsNone(D.path_from_poslist("1 2 3", issues))
        self.assertEqual(issues, ["invalid_geometry"])


class IntervalTest(unittest.TestCase):
    start, end = utc("2026-06-01T00:00:00"), utc("2026-12-24T22:59:00")

    def period(self, frm="20:00:00.0000000+01:00", to="06:00:00.0000000+01:00", days=()):
        return dict(start_at=self.start, end_at=self.end, time_from=frm, time_to=to, days=list(days))

    def spans(self, periods, day=DAY, start=None, end=None):
        return [(i["start_at"], i["end_at"]) for i in D.active_intervals(start or self.start, end or self.end, periods, day)]

    def test_no_window_means_the_whole_day(self):
        self.assertEqual(self.spans([]), [(utc("2026-11-05T00:00:00"), utc("2026-11-06T00:00:00"))])

    def test_overall_bounds_clip(self):
        self.assertEqual(self.spans([], start=utc("2026-11-05T10:00:00"), end=utc("2026-11-05T12:00:00")),
                         [(utc("2026-11-05T10:00:00"), utc("2026-11-05T12:00:00"))])
        self.assertEqual(self.spans([], start=utc("2026-11-07T10:00:00"), end=utc("2026-11-08T12:00:00")), [])

    def test_night_window_is_local_and_crosses_midnight(self):
        # Thursday 5 Nov UTC: Wednesday night runs until 05:00 UTC, Thursday's starts at 19:00 UTC.
        self.assertEqual(self.spans([self.period()]), [(utc("2026-11-05T00:00:00"), utc("2026-11-05T05:00:00")),
                                                        (utc("2026-11-05T19:00:00"), utc("2026-11-06T00:00:00"))])

    def test_weekdays_apply_to_the_night_they_start_on(self):
        self.assertEqual(self.spans([self.period(days=("wednesday",))]), [(utc("2026-11-05T00:00:00"), utc("2026-11-05T05:00:00"))])
        self.assertEqual(self.spans([self.period(days=("monday",))]), [])

    def test_applicable_days_without_a_time_are_whole_local_days(self):
        # Thursday in Oslo is 4 Nov 23:00 to 5 Nov 23:00 UTC; Friday starts within this UTC day.
        self.assertEqual(self.spans([self.period(None, None, days=("thursday",))]), [(utc("2026-11-05T00:00:00"), utc("2026-11-05T23:00:00"))])
        self.assertEqual(self.spans([self.period(None, None, days=("friday",))]), [(utc("2026-11-05T23:00:00"), utc("2026-11-06T00:00:00"))])
        self.assertEqual(self.spans([self.period(None, None, days=("monday",))]), [])

    def test_summer_time_window(self):
        summer = date(2026, 7, 1)
        self.assertEqual(self.spans([self.period("07:00:00.0000000+01:00", "16:00:00.0000000+01:00")], day=summer),
                         [(utc("2026-07-01T05:00:00"), utc("2026-07-01T14:00:00"))])

    def test_overlapping_windows_merge(self):
        periods = [self.period("08:00:00+01:00", "12:00:00+01:00"), self.period("10:00:00+01:00", "14:00:00+01:00")]
        self.assertEqual(self.spans(periods), [(utc("2026-11-05T07:00:00"), utc("2026-11-05T13:00:00"))])

    def test_no_start_has_no_known_interval(self):
        self.assertEqual(D.active_intervals(None, None, [], DAY), [])


class ParseTest(unittest.TestCase):
    def decode(self, feed, *rows):
        return list(D.decode_partition(rows, feed))

    def test_full_record_once_sighting_every_poll(self):
        xml = situation(record())
        rows = self.decode("situation", bronze_row(xml), bronze_row(xml, received="2026-11-05T12:05:00.000Z", digest="other"))
        self.assertEqual([r["kind"] for r in rows], ["record", "sighting", "sighting"])
        full = rows[0]
        self.assertEqual((full["record_id"], full["version"], full["record_type"]), ("r1", "2", "MaintenanceWorks"))
        self.assertEqual(full["created_at"], datetime(2026, 6, 1, 6, 11, 5, 806893, tzinfo=timezone.utc))
        self.assertEqual(full["comment"], "Vegarbeid.|Fartsgrense 50 km/t.")     # never translated
        self.assertEqual(full["publication_at"], datetime(2026, 11, 5, 12, 1, 7, 165000, tzinfo=timezone.utc))

    def test_same_id_and_version_with_another_version_time_is_another_measure(self):
        second = record().replace("2026-06-09T12:52:15.6464439", "2026-06-09T12:52:15.6470000").replace(
            "<ns12:severity>low</ns12:severity>",
            "<ns12:severity>low</ns12:severity><ns12:roadOrCarriagewayOrLaneManagementType>roadClosed</ns12:roadOrCarriagewayOrLaneManagementType>")
        rows = [r for r in self.decode("situation", bronze_row(situation(record(), second))) if r["kind"] == "record"]
        self.assertEqual(len(rows), 2)
        self.assertEqual([r["record_subtypes"] for r in rows], [[], ["roadClosed"]])

    def test_location_keeps_coordinates_and_drops_alert_c_duplicates(self):
        full = self.decode("situation", bronze_row(situation(record())))[0]
        self.assertEqual(len(full["locations"]), 1)
        location = full["locations"][0]
        self.assertEqual((location["kind"], location["road_number"], location["county_code"], location["length_m"]),
                         ("LinearLocation", "F168", "32", 1585.0))
        self.assertEqual(location["path"][0], dict(latitude=59.948799, longitude=10.606923))
        self.assertEqual(len(location["path"]), 2)

    def test_recurring_window_reaches_the_active_intervals(self):
        full = self.decode("situation", bronze_row(situation(record(validity=NIGHTS))))[0]
        self.assertEqual(full["valid_periods"][0]["days"], ["monday", "wednesday"])
        self.assertEqual([(i["start_at"], i["end_at"]) for i in full["active_intervals"]],
                         [(utc("2026-11-05T00:00:00"), utc("2026-11-05T05:00:00"))])

    def test_section_without_vehicles_keeps_its_row(self):
        rows = {r["section_id"]: r for r in self.decode("traveltime", bronze_row(TRAVEL))}
        self.assertEqual((rows["100357"]["travel_time_s"], rows["100357"]["traffic_status"]), (None, "unknown"))
        self.assertEqual((rows["100356"]["travel_time_s"], rows["100356"]["traffic_status"]), (119.0, "freeFlow"))
        self.assertEqual(rows["100357"]["period_start_at"], datetime(2026, 11, 5, 2, 55, 0, 10000, tzinfo=timezone.utc))

    def test_weather_columns_main_sensor_and_measurement_time(self):
        row = self.decode("weather", bronze_row(container(site_measurements())))[0]
        self.assertEqual((row["site_id"], row["air_temperature_c"], row["relative_humidity_pct"]), ("1629006", 6.0, 72.9))
        self.assertEqual(row["road_surface_temperature_c"], 5.4)    # index 801 before 802
        self.assertEqual((row["wind_speed"], row["visibility_m"], row["precipitation_mm_h"], row["friction"], row["road_condition"]),
                         (2.6, 20000.0, 0.4, 0.79, "wet"))
        self.assertEqual(row["measurement_at"], datetime(2026, 11, 5, 2, 41, tzinfo=timezone.utc))
        self.assertEqual(row["dq_issue"], [])

    def test_unknown_weather_quantity_is_visible(self):
        xml = container(f'<ns6:siteMeasurements><ns6:measurementSiteReference id="1"/>{quantity(9, "<ns6:wind><gale><v>1</v></gale></ns6:wind>")}</ns6:siteMeasurements>')
        self.assertEqual(self.decode("weather", bronze_row(xml))[0]["dq_issue"], ["unknown_weather_quantity_gale"])

    def test_snapshot_without_content_is_a_contract_failure(self):
        for feed in ("situation", "weather", "traveltime", "weather-sites", "traveltime-locations"):
            rows = self.decode(feed, bronze_row(container("<error>no data</error>")))
            self.assertEqual([(r["kind"], r["dq_issue"]) for r in rows], [("quarantine", ["unknown_message_shape"])], feed)

    def test_non_numeric_quantity_index_is_flagged(self):
        xml = container(site_measurements().replace('index="101"', 'index="x"'))
        row = self.decode("weather", bronze_row(xml))[0]
        self.assertEqual((row["air_temperature_c"], row["dq_issue"]), (6.0, ["invalid_quantity_index"]))

    def test_references(self):
        site = self.decode("weather-sites", bronze_row(SITES))[0]
        self.assertEqual((site["id"], site["name"], site["road_number"], site["county_code"], site["latitude"]),
                         ("1629006", "Fv 714 Våvatnet", "F714", "50", 63.309277))
        section = self.decode("traveltime-locations", bronze_row(SECTIONS))[0]
        self.assertEqual((section["id"], section["name"], len(section["path"])), ("100357", "Åsanevegen - Eikås", 2))

    def test_failed_polls_and_repeats_are_not_data(self):
        rows = self.decode("traveltime", bronze_row(None, status="fetch_error"), bronze_row(TRAVEL, digest="same"), bronze_row(TRAVEL, digest="same"))
        self.assertEqual(len(rows), 2)

    def test_unusable_lines_are_quarantined(self):
        rows = self.decode("weather", bronze_row("<broken", digest="a"), bronze_row(None, status="corrupt", digest="b"))
        self.assertEqual([r["dq_issue"] for r in rows], [["malformed_payload"], ["bronze_corrupt"]])
        self.assertEqual({r["kind"] for r in rows}, {"quarantine"})


@unittest.skipUnless(find_spec("pyspark"), "pyspark not installed")
class SparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import silver_datex
        from pyspark.sql import SparkSession
        cls.J = silver_datex
        cls.spark = SparkSession.builder.master("local[2]").appName("silver-datex-tests") \
            .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2").config("spark.sql.parquet.compression.codec", "zstd") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="silver-datex-"))
        self.output = self.tmp / "out"

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def bronze(self, feed, rows, hours=None, day=DAY):
        """A Bronze partition: one row per entry, in all 24 hours unless hours is given."""
        rows = [dict(r, hour=(hours[i] if hours else f"{i:02d}"), _bronze_source_file=r["_bronze_source_file"]) for i, r in enumerate(rows)]
        columns = "received_at string, payload string, hour string, _bronze_source_file string, _bronze_payload_sha256 string, _bronze_run_id string, _bronze_status string"
        self.spark.createDataFrame([{k: r[k] for k in r if k != "source_date"} for r in rows], columns) \
            .write.parquet(str(self.output / "bronze" / f"datex-{feed}" / f"date={day}"))

    def full_day(self):
        hours = [f"{h:02d}" for h in range(24)]
        polls = [bronze_row(situation(record(validity=NIGHTS), record(rid="r2", kind="Accident")), received=f"2026-11-05T{h}:00:00.000Z", digest="s" + h) for h in hours]
        self.bronze("situation", polls)
        self.bronze("traveltime", [bronze_row(TRAVEL, received=f"2026-11-05T{h}:00:00.000Z", digest="t" + h) for h in hours])
        current = "".join(site_measurements(at=f"2026-11-05T{h}:40:00.000+01:00") for h in hours[:1])
        broken = site_measurements(site="2", at="2026-11-05T01:00:00.000+01:00", air=-39.9, surface=5.0)
        frozen = site_measurements(site="3", at="2025-01-01T01:00:00.000+01:00")
        self.bronze("weather", [bronze_row(container(current + broken + frozen), received=f"2026-11-05T{h}:00:00.000Z", digest="w" + h) for h in hours])
        self.bronze("traveltime-locations", [bronze_row(SECTIONS, digest="l")], hours=["05"])
        self.bronze("weather-sites", [bronze_row(SITES, digest="m")], hours=["05"])

    def quays(self):
        """Quay A beside r1's line and the travel time section's far end is elsewhere; B is far from all."""
        self.spark.createDataFrame([("NSR:Quay:A", 59.9488, 10.6075), ("NSR:Quay:B", 70.0, 25.0), ("NSR:Quay:C", 60.4682, 5.3264), ("NSR:Quay:D", None, None)],
                                   "id string, latitude double, longitude double") \
            .write.parquet(str(self.output / "silver" / "quay" / f"operating_date={DAY}"))

    def table(self, name):
        return self.spark.read.parquet(str(self.output / "silver" / name / f"observation_date={DAY}"))

    def test_full_day_grains_flags_and_marker(self):
        self.full_day()
        result = self.J.run(self.tmp, self.output, DAY, spark=self.spark)
        self.assertEqual((result["status"], result["ready"]), ("complete", True))
        marker = json.loads((self.output / "silver" / "_runs" / "datex" / f"observation_date={DAY}" / "latest.json").read_text())
        self.assertEqual((marker["run_id"], marker["ready"]), (result["run_id"], True))

        situations = {r["record_id"]: r for r in self.table("road_situation").collect()}
        self.assertEqual(sorted(situations), ["r1", "r2"])
        self.assertEqual(situations["r1"]["snapshots_seen"], 24)
        self.assertEqual((situations["r1"]["first_seen_at"], situations["r1"]["last_seen_at"]),
                         (datetime(2026, 11, 5, 0, 0), datetime(2026, 11, 5, 23, 0)))
        intervals = self.table("road_situation_interval").collect()
        self.assertEqual(sorted((r["record_id"], r["start_at"].hour) for r in intervals), [("r1", 0), ("r2", 0)])
        self.assertEqual({r["end_at"].hour for r in intervals if r["record_id"] == "r1"}, {5})

        weather = {r["site_id"]: r for r in self.table("road_weather").collect()}
        self.assertEqual(sorted(weather), ["1629006", "2", "3"])
        self.assertIn("suspect_air_temperature", weather["2"]["dq_issue"])
        self.assertIn("stale_measurement", weather["3"]["dq_issue"])
        self.assertEqual(weather["1629006"]["dq_issue"], [])

        times = self.table("road_travel_time").collect()
        self.assertEqual(len(times), 2)     # 24 polls of the same two periods
        unmeasured = next(r for r in times if r["section_id"] == "100357")
        self.assertEqual((unmeasured["has_measurement"], unmeasured["congestion_ratio"]), (False, None))

        site = self.table("weather_site").first()
        self.assertEqual((site["id"], site["measurements"], site["usable"]), ("1629006", 1, False))
        section = self.table("road_section").first()
        self.assertIn("never_measured", section["dq_issue"])

    def test_rows_without_their_key_are_quarantined(self):
        self.full_day()
        bad_time = record(rid="r3").replace("2026-06-09T12:52:15.6464439+02:00", "yesterday")
        undated = site_measurements(site="4", at="")
        for feed, payload in (("situation", situation(record(), bad_time)), ("weather", container(site_measurements() + undated))):
            shutil.rmtree(self.output / "bronze" / f"datex-{feed}" / f"date={DAY}")
            self.bronze(feed, [bronze_row(payload, received=f"2026-11-05T{h:02d}:00:00.000Z", digest=f"{feed}{h}") for h in range(24)])
        result = self.J.run(self.tmp, self.output, DAY, spark=self.spark)
        self.assertEqual((result["status"], result["ready"]), ("complete", True))
        self.assertEqual([r["record_id"] for r in self.table("road_situation").collect()], ["r1"])
        self.assertEqual([r["site_id"] for r in self.table("road_weather").collect()], ["1629006"])
        rows = self.table("datex_quarantine").collect()
        situations = [r for r in rows if r["feed"] == "situation"]
        self.assertTrue(situations and all(r["dq_issue"] == ["invalid_version_at", "missing_version_at"] for r in situations))
        self.assertEqual(json.loads(situations[0]["entity_json"])["record_id"], "r3")
        self.assertEqual([r["dq_issue"] for r in rows if r["feed"] == "weather"], [["missing_measurement_at"]] * 24)

    def test_spatial_links_stay_within_the_radius_and_on_their_grain(self):
        self.full_day()
        self.quays()
        result = self.J.run(self.tmp, self.output, DAY, spark=self.spark)
        self.assertEqual(result["quay_snapshot_date"], str(DAY))
        situations = {(r["quay_id"], r["record_id"]) for r in self.table("quay_road_situation").collect()}
        self.assertEqual(situations, {("NSR:Quay:A", "r1"), ("NSR:Quay:A", "r2")})
        distance = self.table("quay_road_situation").first()["distance_m"]
        self.assertLess(distance, 100)
        self.assertEqual([(r["quay_id"], r["section_id"]) for r in self.table("quay_road_section").collect()], [("NSR:Quay:C", "100357")])
        self.assertEqual(self.table("quay_weather_site").count(), 0)     # no station measured enough that day

    def test_nearest_usable_station(self):
        quays = self.spark.createDataFrame([("Q1", 63.3, 9.5), ("Q2", 69.0, 20.0)], "quay_id string, quay_latitude double, quay_longitude double")
        sites = self.spark.createDataFrame([("near", 63.31, 9.57, True), ("nearer_but_dead", 63.3, 9.5, False)],
                                           "id string, latitude double, longitude double, usable boolean")
        rows = {r["quay_id"]: r for r in self.J.build_quay_weather_site(quays, sites).collect()}
        self.assertEqual((rows["Q1"]["site_id"], rows["Q1"]["dq_issue"]), ("near", []))
        self.assertEqual(rows["Q2"]["dq_issue"], ["nearest_site_beyond_30km"])

    def test_missing_hours_block_a_strict_run_and_keep_the_published_day(self):
        self.full_day()
        first = self.J.run(self.tmp, self.output, DAY, spark=self.spark)
        shutil.rmtree(self.output / "bronze" / "datex-weather" / f"date={DAY}")
        self.bronze("weather", [bronze_row(container(site_measurements()), digest="x")], hours=["05"])
        with self.assertRaisesRegex(ValueError, "incomplete datex-weather"):
            self.J.run(self.tmp, self.output, DAY, spark=self.spark)
        marker = json.loads((self.output / "silver" / "_runs" / "datex" / f"observation_date={DAY}" / "latest.json").read_text())
        self.assertEqual(marker["run_id"], first["run_id"])
        partial = self.J.run(self.tmp, self.output, DAY, spark=self.spark, allow_partial=True)
        self.assertEqual((partial["status"], partial["ready"]), ("partial", False))

    def test_malformed_snapshot_blocks_readiness(self):
        self.full_day()
        shutil.rmtree(self.output / "bronze" / "datex-traveltime" / f"date={DAY}")
        self.bronze("traveltime", [bronze_row("<broken", digest=f"b{h}", received=f"2026-11-05T{h:02d}:00:00.000Z") for h in range(24)])
        with self.assertRaisesRegex(ValueError, "source contract errors"):
            self.J.run(self.tmp, self.output, DAY, spark=self.spark)


if __name__ == "__main__":
    unittest.main()
