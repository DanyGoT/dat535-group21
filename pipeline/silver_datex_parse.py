"""Pure, task-side DATEX decoders. No network access and no Spark dependency.

One Bronze row is one poll: a full snapshot of a feed. Decoders emit narrow, typed records
with UTC times. Norwegian message text stays untranslated (NPRA's terms).
"""

import io
import math
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from silver_parse import local, number, reject, strip_ns, text, timestamp

OSLO = ZoneInfo("Europe/Oslo")
XSI_TYPE = "{http://www.w3.org/2001/XMLSchema-instance}type"
WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

# Transverse Mercator inverse (Krueger series) on the GRS80 ellipsoid, UTM zone 33 north. Both
# EPSG:25833 (situations) and EPSG:32633 (travel time sections) use it: ETRS89 and WGS84 differ
# by less than a metre here. pyproj is not installed on the VM and three constants don't need it.
_F = 1 / 298.257222101
_N = _F / (2 - _F)
_RECTIFYING = 6378137.0 / (1 + _N) * (1 + _N ** 2 / 4 + _N ** 4 / 64)
_BETA = (_N / 2 - 2 * _N ** 2 / 3 + 37 * _N ** 3 / 96, _N ** 2 / 48 + _N ** 3 / 15, 17 * _N ** 3 / 480)
_DELTA = (2 * _N - 2 * _N ** 2 / 3 - 2 * _N ** 3, 7 * _N ** 2 / 3 - 8 * _N ** 3 / 5, 56 * _N ** 3 / 15)


def utm33_to_wgs84(easting, northing):
    """(latitude, longitude) in degrees. Western Norway has negative eastings: that is valid."""
    xi = northing / (0.9996 * _RECTIFYING)
    eta = (easting - 500000.0) / (0.9996 * _RECTIFYING)
    xi_p = xi - sum(b * math.sin(2 * j * xi) * math.cosh(2 * j * eta) for j, b in enumerate(_BETA, 1))
    eta_p = eta - sum(b * math.cos(2 * j * xi) * math.sinh(2 * j * eta) for j, b in enumerate(_BETA, 1))
    chi = math.asin(math.sin(xi_p) / math.cosh(eta_p))
    latitude = chi + sum(d * math.sin(2 * j * chi) for j, d in enumerate(_DELTA, 1))
    return round(math.degrees(latitude), 6), round(15.0 + math.degrees(math.atan2(math.sinh(eta_p), math.cos(xi_p))), 6)


def distance_m(a, b):
    """Great-circle metres between two {latitude, longitude}."""
    lat1, lat2 = math.radians(a["latitude"]), math.radians(b["latitude"])
    x = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(math.radians(b["longitude"] - a["longitude"]) / 2) ** 2
    return 2 * 6371000 * math.asin(math.sqrt(x))


def along_path(path, step_m=150.0):
    """Points every ~step_m along a line: vertices can be 2 m apart in a bend and kilometres
    apart in a tunnel, and a distance to vertices alone would miss a quay beside a long segment."""
    if not path:
        return []
    points = [path[0]]
    since = 0.0
    for a, b in zip(path, path[1:]):
        length = distance_m(a, b)
        if since + length < step_m:
            since += length
            continue
        pieces = max(1, int((since + length) // step_m))
        for i in range(1, pieces + 1):
            t = i / pieces
            points.append(dict(latitude=a["latitude"] + t * (b["latitude"] - a["latitude"]),
                               longitude=a["longitude"] + t * (b["longitude"] - a["longitude"])))
        since = 0.0
    if points[-1] != path[-1]:
        points.append(path[-1])
    return points


def path_from_poslist(value, issues):
    """'e n e n ...' in UTM 33 -> [{latitude, longitude}]."""
    if not value:
        return None
    try:
        numbers = [float(x) for x in value.split()]
        if len(numbers) % 2:
            raise ValueError("odd coordinate count")
        return [dict(zip(("latitude", "longitude"), utm33_to_wgs84(numbers[i], numbers[i + 1])))
                for i in range(0, len(numbers), 2)]
    except ValueError:
        issues.append("invalid_geometry")
        return None


def value_text(elem, path):
    """DATEX multilingual text: the first <values><value> under path."""
    node = elem.find(path)
    return text(node, "values/value") if node is not None else None


def wall_clock(value):
    """'07:00:00.0000000+01:00' -> time(7). NPRA serialises every time of day with today's offset,
    also for records whose dates carry +02:00, so the clock is the Oslo wall clock."""
    return time.fromisoformat(value[:8])


def active_intervals(start, end, periods, day):
    """When a record applies during one UTC day: [{start_at, end_at}], merged and sorted.

    overallStartTime..overallEndTime only bounds a record: 42 % of them apply in recurring
    windows (e.g. 20:00-06:00, Monday-Thursday). A window that crosses midnight belongs to the
    weekday it starts on, and applicable days without a time of day mean the whole local day. A
    record without a start time has no known interval.
    """
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    if start is None:
        return []
    lower, upper = max(start, day_start), min(end, day_end) if end else day_end
    spans = []
    for period in periods or [dict(start_at=None, end_at=None, time_from=None, time_to=None, days=[])]:
        low = max(lower, period["start_at"]) if period["start_at"] else lower
        high = min(upper, period["end_at"]) if period["end_at"] else upper
        if (period["time_from"] and period["time_to"]) or period["days"]:
            # From midnight to midnight: finish <= begin below makes it the next day's midnight.
            time_from, time_to = period["time_from"] or "00:00:00", period["time_to"] or "00:00:00"
            for offset in (-1, 0, 1):
                local_day = day + timedelta(days=offset)
                if period["days"] and WEEKDAYS[local_day.weekday()] not in period["days"]:
                    continue
                begin = datetime.combine(local_day, wall_clock(time_from), OSLO)
                finish = datetime.combine(local_day, wall_clock(time_to), OSLO)
                if finish <= begin:
                    finish = datetime.combine(local_day + timedelta(days=1), wall_clock(time_to), OSLO)
                spans.append((max(low, begin.astimezone(timezone.utc)), min(high, finish.astimezone(timezone.utc))))
        else:
            spans.append((low, high))
    merged = []
    for begin, finish in sorted(s for s in spans if s[0] < s[1]):
        if merged and begin <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], finish))
        else:
            merged.append((begin, finish))
    return [dict(start_at=b, end_at=f) for b, f in merged]


def parse_location(item, issues):
    """One location of a record; None for the Alert-C duplicates that carry no coordinates."""
    display = item.find("coordinatesForDisplay")
    point = display if display is not None else item.find("pointByCoordinates/pointCoordinates")
    path = path_from_poslist(text(item, "gmlLineString/posList"), issues)
    if point is None and path is None:
        return None
    described = item.find("supplementaryPositionalDescription")
    described = described if described is not None else ET.Element("none")
    return dict(kind=(item.get(XSI_TYPE) or "").split(":")[-1],
                latitude=number(text(point, "latitude"), issues, "latitude") if point is not None else None,
                longitude=number(text(point, "longitude"), issues, "longitude") if point is not None else None,
                length_m=number(text(described, "lengthAffected"), issues, "length_affected"),
                road_number=text(described, "roadInformation/roadNumber"),
                road_name=text(described, "roadInformation/roadName"),
                county_code=text(described, "namedArea/subdivisionCode"),
                county=value_text(described, "namedArea/areaName"),
                description=value_text(described, "locationDescription"), path=path)


def parse_record(situation, record, base, source_day):
    issues = list(base["dq_issue"])
    validity = record.find("validity/validityTimeSpecification")
    if validity is None:
        validity = ET.Element("none")
    start = timestamp(text(validity, "overallStartTime"), issues, "start_at")
    end = timestamp(text(validity, "overallEndTime"), issues, "end_at")
    periods = []
    for period in validity.findall("validPeriod"):
        periods.append(dict(start_at=timestamp(text(period, "startOfPeriod"), issues, "period_start"),
                            end_at=timestamp(text(period, "endOfPeriod"), issues, "period_end"),
                            time_from=text(period, "recurringTimePeriodOfDay/startTimeOfPeriod"),
                            time_to=text(period, "recurringTimePeriodOfDay/endTimeOfPeriod"),
                            days=[(e.text or "").strip() for e in period.findall("recurringDayWeekMonthPeriod/applicableDay")]))
    comments = {text(c, "commentType"): value_text(c, "comment") for c in record.findall("generalPublicComment")}
    reference = record.find("locationReference")
    items = [] if reference is None else (reference.findall("locationContainedInGroup") or [reference])
    locations = [loc for loc in (parse_location(item, issues) for item in items) if loc]
    try:
        intervals = active_intervals(start, end, periods, source_day)
    except (ValueError, TypeError):
        issues.append("invalid_recurring_period")
        intervals = []
    cause = record.find("cause/detailedCauseType")
    # What the record type means concretely: roadClosed, temporaryTrafficLights, roadworks, ...
    subtypes = [(child.text or "").strip() for child in record if child.tag.endswith("Type") and child.text]
    return dict(base, kind="record", dq_issue=issues, record_id=record.get("id"), version=record.get("version"),
                situation_id=situation.get("id"), overall_severity=text(situation, "overallSeverity"),
                record_type=(record.get(XSI_TYPE) or "").split(":")[-1], record_subtypes=subtypes,
                vehicle_usage=text(record, "vehicleUsage"),
                created_at=timestamp(text(record, "situationRecordCreationTime"), issues, "created_at"),
                version_at=timestamp(text(record, "situationRecordVersionTime"), issues, "version_at"),
                severity=text(record, "severity"), probability=text(record, "probabilityOfOccurrence"),
                safety_related=text(record, "safetyRelatedMessage") == "true",
                compliance=text(record, "complianceOption"),
                lanes_restricted=number(text(record, "impact/numberOfLanesRestricted"), issues, "lanes_restricted", integer=True),
                detailed_cause=(cause[0].text or "").strip() if cause is not None and len(cause) else None,
                roadworks_id=text(record, "roadworksIdentifier"), maintenance_type=text(record, "roadMaintenanceType"),
                start_at=start, end_at=end, valid_periods=periods, active_intervals=intervals,
                comment=comments.get("dataProcessingNote"), description=comments.get("description"),
                locations=locations)


def parse_situation(payload, base, source_day):
    """A sighting per record per poll, the full record only the first time this task sees that
    version: a day is ~690,000 sightings of ~3,000 versions, and the geometry is the bulk.

    (id, version) is not unique: ~1.5 % of records repeat it within one situation for a second
    measure at the same place (traffic lights and manual direction, road closed and emergency
    vehicles only). Their version times differ, so the key includes it.
    """
    seen = base.pop("_seen")
    publication = None
    for _, elem in ET.iterparse(io.BytesIO(payload.encode("utf-8")), events=("end",)):
        name = local(elem.tag)
        if name == "publicationTime" and publication is None:
            publication = timestamp(elem.text, base["dq_issue"], "publication_at")
        elif name == "situation":
            strip_ns(elem)
            for record in elem.findall("situationRecord"):
                key = (record.get("id"), record.get("version"), text(record, "situationRecordVersionTime"))
                if key not in seen:
                    seen.add(key)
                    yield dict(parse_record(elem, record, base, source_day), publication_at=publication)
                yield dict(base, kind="sighting", record_id=key[0], version=key[1], publication_at=publication,
                           version_at=timestamp(key[2], [], "version_at"))
            elem.clear()


def parse_traveltime(payload, base):
    """One row per road section and 5-minute period. A section with no vehicles measured keeps
    its row, with status 'unknown' and no travel time: that is an observation, not a gap."""
    root = strip_ns(ET.fromstring(payload))
    publication = timestamp(text(root, "payload/publicationTime"), base["dq_issue"], "publication_at")
    measured, status = {}, {}
    for quantity in root.iter("physicalQuantity"):
        ref = quantity.find("pertinentLocation/predefinedLocationReference")
        data = quantity.find("basicData")
        if ref is None or data is None:
            continue
        section = ref.get("id")
        kind = (data.get(XSI_TYPE) or "").split(":")[-1]
        if kind == "TrafficStatus":
            status[section] = text(data, "trafficStatus/trafficStatusValue")
        elif kind == "TravelTimeData":
            issues = list(base["dq_issue"])
            measured[section] = dict(
                base, kind="value", dq_issue=issues, section_id=section, section_version=ref.get("version"),
                publication_at=publication,
                period_start_at=timestamp(text(data, "measurementOrCalculationTime/period/startOfPeriod"), issues, "period_start"),
                period_end_at=timestamp(text(data, "measurementOrCalculationTime/period/endOfPeriod"), issues, "period_end"),
                travel_time_s=number(text(data, "travelTime/duration"), issues, "travel_time"),
                free_flow_s=number(text(data, "freeFlowTravelTime/duration"), issues, "free_flow"),
                free_flow_kmh=number(text(data, "freeFlowSpeed/speed"), issues, "free_flow_speed"),
                trend=text(data, "travelTimeTrendType"), time_type=text(data, "travelTimeType"))
    for section, row in measured.items():
        yield dict(row, traffic_status=status.get(section))


# Path of a measurement inside basicData -> column. The lowest index of a quantity at a site is
# its main sensor (801, then 802/803 for further lanes). Units as published; the wind speed and
# thickness units are UNVERIFIED (docs/datex/content.md).
WEATHER_QUANTITIES = {
    "airTemperature": "air_temperature_c", "relativeHumidity": "relative_humidity_pct",
    "dewPointTemperature": "dew_point_c", "protectionTemperature": "protection_temperature_c",
    "roadSurfaceTemperature": "road_surface_temperature_c", "minimumTemperature": "air_temperature_min_c",
    "maximumTemperature": "air_temperature_max_c", "windSpeed": "wind_speed", "maximumWindSpeed": "wind_gust",
    "windDirectionBearing": "wind_direction_deg", "minimumVisibilityDistance": "visibility_m",
    "precipitationIntensity": "precipitation_mm_h", "waterFilmThickness": "water_film", "iceLayerThickness": "ice_layer",
    "depthOfSnow": "snow_depth", "friction": "friction", "temperatureBelowOrAboveRoadSurface": "subsurface_temperature_c",
    "weatherRelatedRoadConditionType": "road_condition"}
# Containers between basicData and the quantity's own element.
_WEATHER_GROUPS = {"temperature", "humidity", "wind", "visibility", "precipitationDetail", "roadSurfaceConditionMeasurements"}


def weather_quantity(data):
    """(quantity element name, its one leaf value) of one basicData. Some measurements carry
    their own measurementOrCalculationTime as a sibling; the site's default time covers them."""
    children = [c for c in data if c.tag != "measurementOrCalculationTime"]
    while len(children) == 1 and children[0].tag in _WEATHER_GROUPS:
        children = list(children[0])
    if not children:
        return None, None
    quantity = children[0]
    leaf = quantity
    while len(leaf):
        leaf = leaf[0]
    return quantity.tag, (leaf.text or "").strip() or None


def parse_weather(payload, base):
    """One row per station and measurement time, all its quantities as columns."""
    root = strip_ns(ET.fromstring(payload))
    publication = timestamp(text(root, "payload/publicationTime"), base["dq_issue"], "publication_at")
    for site in root.iter("siteMeasurements"):
        issues = list(base["dq_issue"])
        ref = site.find("measurementSiteReference")
        row = dict(base, kind="value", dq_issue=issues, site_id=ref.get("id") if ref is not None else None,
                   site_version=ref.get("version") if ref is not None else None, publication_at=publication,
                   measurement_at=timestamp(text(site, "measurementTimeDefault/timeValue"), issues, "measurement_at"))
        taken = set()
        for quantity in sorted(site.findall("physicalQuantity"), key=lambda q: int(q.get("index") or 0)):
            data = quantity.find("physicalQuantity/basicData")
            if data is None:
                continue
            name, value = weather_quantity(data)
            column = WEATHER_QUANTITIES.get(name)
            if column is None:
                # A new element must be visible: the source changed its shape.
                issues.append(f"unknown_weather_quantity_{name}")
            elif column not in taken:
                taken.add(column)
                row[column] = value if column == "road_condition" else number(value, issues, column)
        yield row


def parse_weather_sites(payload, base):
    root = strip_ns(ET.fromstring(payload))
    publication = timestamp(text(root, "payload/publicationTime"), base["dq_issue"], "publication_at")
    for site in root.iter("measurementSite"):
        issues = list(base["dq_issue"])
        location = site.find("measurementSiteLocation")
        location = location if location is not None else ET.Element("none")
        yield dict(base, kind="site", dq_issue=issues, publication_at=publication, id=site.get("id"), version=site.get("version"),
                   name=value_text(site, "measurementSiteName"),
                   latitude=number(text(location, "pointByCoordinates/pointCoordinates/latitude"), issues, "latitude"),
                   longitude=number(text(location, "pointByCoordinates/pointCoordinates/longitude"), issues, "longitude"),
                   road_number=text(location, "supplementaryPositionalDescription/roadInformation/roadNumber"),
                   county_code=text(location, "supplementaryPositionalDescription/namedArea/subdivisionCode"),
                   county=value_text(location, "supplementaryPositionalDescription/namedArea/areaName"))


def parse_sections(payload, base):
    root = strip_ns(ET.fromstring(payload))
    publication = timestamp(text(root, "payload/publicationTime"), base["dq_issue"], "publication_at")
    for section in root.iter("predefinedLocationReference"):
        if not len(section):
            continue
        issues = list(base["dq_issue"])
        path = path_from_poslist(text(section, "location/gmlLineString/posList"), issues)
        yield dict(base, kind="section", dq_issue=issues, publication_at=publication, id=section.get("id"),
                   version=section.get("version"), name=value_text(section, "predefinedLocationName"), path=path)


def situation_points(rows):
    """mapInArrow body: one row per point of each situation location, for the spatial links."""
    for row in rows:
        for location in row["locations"] or []:
            points = along_path(location["path"]) or ([dict(latitude=location["latitude"], longitude=location["longitude"])]
                                                      if location["latitude"] is not None and location["longitude"] is not None else [])
            for point in points:
                yield dict(record_id=row["record_id"], version=row["version"], version_at=row["version_at"], **point)


def section_points(rows):
    for row in rows:
        for point in along_path(row["path"]):
            yield dict(section_id=row["id"], **point)


def decode_partition(rows, feed):
    seen = set()    # one task's situation versions (parse_situation), or snapshot hashes
    for source in rows:
        issues = []
        base = dict(kind=None, received_at=timestamp(source["received_at"], issues, "received_at"),
                    source_file=source["_bronze_source_file"], payload_sha256=source["_bronze_payload_sha256"],
                    bronze_run_id=source["_bronze_run_id"], dq_issue=issues)
        status = source["_bronze_status"]
        if status == "fetch_error":
            # Evidence of when we could not see NPRA; Bronze's run record counts them.
            continue
        if status not in ("ok", "not_modified"):
            yield reject(dict(base, record_sha256=base["payload_sha256"]), "bronze_" + status)
            continue
        if status == "not_modified" or source["payload"] is None:
            continue
        try:
            if feed == "situation":
                yield from parse_situation(source["payload"], dict(base, _seen=seen), date.fromisoformat(source["source_date"]))
            elif source["_bronze_payload_sha256"] in seen:
                continue    # polled at twice NPRA's cadence: most snapshots repeat byte for byte
            else:
                seen.add(source["_bronze_payload_sha256"])
                parser = {"traveltime": parse_traveltime, "weather": parse_weather,
                          "weather-sites": parse_weather_sites, "traveltime-locations": parse_sections}[feed]
                yield from parser(source["payload"], base)
        except ET.ParseError:
            yield reject(dict(base, record_sha256=base["payload_sha256"]), "malformed_payload")


def schema(feed):
    """Stable, explicit Spark contracts. Raw payloads never enter these schemas."""
    from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType, StringType, StructField,
                                   StructType, TimestampType)

    def struct(**columns):
        return StructType([StructField(k, v) for k, v in columns.items()])

    point = struct(latitude=DoubleType(), longitude=DoubleType())
    fields = dict(kind=StringType(), received_at=TimestampType(), publication_at=TimestampType(), source_file=StringType(),
                  payload_sha256=StringType(), bronze_run_id=StringType(), dq_issue=ArrayType(StringType()), entity_json=StringType())
    if feed == "situation":
        fields.update({k: StringType() for k in (
            "record_id", "version", "situation_id", "overall_severity", "record_type", "severity", "probability", "compliance",
            "vehicle_usage", "detailed_cause", "roadworks_id", "maintenance_type", "comment", "description")})
        fields.update({k: TimestampType() for k in ("created_at", "version_at", "start_at", "end_at")})
        fields.update(safety_related=BooleanType(), lanes_restricted=IntegerType(), record_subtypes=ArrayType(StringType()),
                      valid_periods=ArrayType(struct(start_at=TimestampType(), end_at=TimestampType(), time_from=StringType(),
                                                     time_to=StringType(), days=ArrayType(StringType()))),
                      active_intervals=ArrayType(struct(start_at=TimestampType(), end_at=TimestampType())),
                      locations=ArrayType(struct(kind=StringType(), latitude=DoubleType(), longitude=DoubleType(), length_m=DoubleType(),
                                                 road_number=StringType(), road_name=StringType(), county_code=StringType(),
                                                 county=StringType(), description=StringType(), path=ArrayType(point))))
    elif feed == "traveltime":
        fields.update(section_id=StringType(), section_version=StringType(), period_start_at=TimestampType(),
                      period_end_at=TimestampType(), travel_time_s=DoubleType(), free_flow_s=DoubleType(),
                      free_flow_kmh=DoubleType(), trend=StringType(), time_type=StringType(), traffic_status=StringType())
    elif feed == "weather":
        fields.update(site_id=StringType(), site_version=StringType(), measurement_at=TimestampType(), road_condition=StringType())
        fields.update({c: DoubleType() for c in WEATHER_QUANTITIES.values() if c != "road_condition"})
    else:
        fields.update({k: StringType() for k in ("id", "version", "name")})
        if feed == "weather-sites":
            fields.update(latitude=DoubleType(), longitude=DoubleType(), road_number=StringType(), county_code=StringType(), county=StringType())
        else:
            fields.update(path=ArrayType(point))
    return StructType([StructField(k, v) for k, v in fields.items()])
