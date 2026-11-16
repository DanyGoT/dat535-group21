"""DATEX decoders that run inside Spark tasks. Plain Python: no network, no Spark.

One Bronze row is one poll: a full snapshot of a feed. Decoders yield narrow, typed records with
UTC times. Norwegian message text stays untranslated (NPRA's terms).
"""

import io
import math
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timedelta, timezone

from silver_parse import OSLO, local, number, reject, strip_ns, text, timestamp

XSI_TYPE = "{http://www.w3.org/2001/XMLSchema-instance}type"
WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

# UTM zone 33 -> WGS84: the inverse Transverse Mercator (Krueger series) on GRS80.
# Situations use EPSG:25833, sections EPSG:32633; they differ by under a metre here.
# Three constants are cheaper than installing pyproj on the VM.
_F = 1 / 298.257222101
_N = _F / (2 - _F)
_RECTIFYING = 6378137.0 / (1 + _N) * (1 + _N**2 / 4 + _N**4 / 64)
_BETA = (
    _N / 2 - 2 * _N**2 / 3 + 37 * _N**3 / 96,
    _N**2 / 48 + _N**3 / 15,
    17 * _N**3 / 480,
)
_DELTA = (
    2 * _N - 2 * _N**2 / 3 - 2 * _N**3,
    7 * _N**2 / 3 - 8 * _N**3 / 5,
    56 * _N**3 / 15,
)

# Weather: element name inside basicData -> column. Units as published; the wind speed and
# thickness units are UNVERIFIED (docs/datex/content.md).
WEATHER_QUANTITIES = {
    "airTemperature": "air_temperature_c",
    "relativeHumidity": "relative_humidity_pct",
    "dewPointTemperature": "dew_point_c",
    "protectionTemperature": "protection_temperature_c",
    "roadSurfaceTemperature": "road_surface_temperature_c",
    "minimumTemperature": "air_temperature_min_c",
    "maximumTemperature": "air_temperature_max_c",
    "windSpeed": "wind_speed",
    "maximumWindSpeed": "wind_gust",
    "windDirectionBearing": "wind_direction_deg",
    "minimumVisibilityDistance": "visibility_m",
    "precipitationIntensity": "precipitation_mm_h",
    "waterFilmThickness": "water_film",
    "iceLayerThickness": "ice_layer",
    "depthOfSnow": "snow_depth",
    "friction": "friction",
    "temperatureBelowOrAboveRoadSurface": "subsurface_temperature_c",
    "weatherRelatedRoadConditionType": "road_condition",
}

# Wrapper elements between basicData and the quantity's own element.
_WEATHER_GROUPS = {
    "temperature", "humidity", "wind", "visibility", "precipitationDetail",
    "roadSurfaceConditionMeasurements",
}

# Stand-in for a missing optional element, so lookups below just find nothing.
NONE = ET.Element("none")


# --- Geometry ---


def utm33_to_wgs84(easting, northing):
    """(latitude, longitude) in degrees. Western Norway has negative eastings: that is valid."""
    xi = northing / (0.9996 * _RECTIFYING)
    eta = (easting - 500000.0) / (0.9996 * _RECTIFYING)
    terms = list(enumerate(_BETA, 1))
    xi_p = xi - sum(b * math.sin(2 * j * xi) * math.cosh(2 * j * eta) for j, b in terms)
    eta_p = eta - sum(b * math.cos(2 * j * xi) * math.sinh(2 * j * eta) for j, b in terms)
    chi = math.asin(math.sin(xi_p) / math.cosh(eta_p))
    latitude = chi + sum(d * math.sin(2 * j * chi) for j, d in enumerate(_DELTA, 1))
    longitude = 15.0 + math.degrees(math.atan2(math.sinh(eta_p), math.cos(xi_p)))
    return round(math.degrees(latitude), 6), round(longitude, 6)


def distance_m(a, b):
    """Great-circle metres between two {latitude, longitude}."""
    lat1, lat2 = math.radians(a["latitude"]), math.radians(b["latitude"])
    d_lat = lat2 - lat1
    d_lon = math.radians(b["longitude"] - a["longitude"])
    x = math.sin(d_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2
    return 2 * 6371000 * math.asin(math.sqrt(x))


def along_path(path, step_m=150.0):
    """Points every ~step_m along a line, so a quay beside a long straight segment is found."""
    # Vertices can be 2 m apart in a bend and kilometres apart in a tunnel.
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
        d_lat = b["latitude"] - a["latitude"]
        d_lon = b["longitude"] - a["longitude"]
        for i in range(1, pieces + 1):
            t = i / pieces
            points.append(dict(
                latitude=a["latitude"] + t * d_lat,
                longitude=a["longitude"] + t * d_lon,
            ))
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
        pairs = zip(numbers[0::2], numbers[1::2])
        return [
            dict(zip(("latitude", "longitude"), utm33_to_wgs84(easting, northing)))
            for easting, northing in pairs
        ]
    except ValueError:
        issues.append("invalid_geometry")
        return None


# --- Situations (road events) ---


def value_text(elem, path):
    """DATEX multilingual text: the first <values><value> under path."""
    node = elem.find(path)
    return text(node, "values/value") if node is not None else None


def wall_clock(value):
    """'07:00:00.0000000+01:00' -> time(7): the Oslo wall clock."""
    # NPRA writes every time of day with today's offset, even on +02:00 dates: ignore it.
    return time.fromisoformat(value[:8])


def active_intervals(start, end, periods, day):
    """When a record applies during one UTC day: [{start_at, end_at}], merged and sorted.

    The overall start..end only bounds a record: 42 % apply in recurring windows,
    e.g. 20:00-06:00 Monday-Thursday. No start time means no known interval.
    """
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    if start is None:
        return []
    lower = max(start, day_start)
    upper = min(end, day_end) if end else day_end

    no_period = dict(start_at=None, end_at=None, time_from=None, time_to=None, days=[])
    spans = []
    for period in periods or [no_period]:
        low = max(lower, period["start_at"]) if period["start_at"] else lower
        high = min(upper, period["end_at"]) if period["end_at"] else upper
        recurring = (period["time_from"] and period["time_to"]) or period["days"]
        if not recurring:
            spans.append((low, high))
            continue

        # Yesterday's window can reach into today; it belongs to the weekday it starts on.
        for offset in (-1, 0, 1):
            local_day = day + timedelta(days=offset)
            if period["days"] and WEEKDAYS[local_day.weekday()] not in period["days"]:
                continue
            # Days without a time of day mean the whole local day (midnight to midnight).
            clock_from = wall_clock(period["time_from"] or "00:00:00")
            clock_to = wall_clock(period["time_to"] or "00:00:00")
            begin = datetime.combine(local_day, clock_from, OSLO)
            finish = datetime.combine(local_day, clock_to, OSLO)
            if finish <= begin:  # crosses midnight
                finish = datetime.combine(local_day + timedelta(days=1), clock_to, OSLO)
            begin_utc = begin.astimezone(timezone.utc)
            finish_utc = finish.astimezone(timezone.utc)
            spans.append((max(low, begin_utc), min(high, finish_utc)))

    # Merge overlapping spans; drop empty ones.
    merged = []
    for begin, finish in sorted(s for s in spans if s[0] < s[1]):
        if merged and begin <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], finish))
        else:
            merged.append((begin, finish))
    return [dict(start_at=b, end_at=f) for b, f in merged]


def parse_location(item, issues):
    """One location of a record; None for the Alert-C duplicates that carry no coordinates."""
    point = item.find("coordinatesForDisplay")
    if point is None:
        point = item.find("pointByCoordinates/pointCoordinates")
    path = path_from_poslist(text(item, "gmlLineString/posList"), issues)
    if point is None and path is None:
        return None

    latitude = longitude = None
    if point is not None:
        latitude = number(text(point, "latitude"), issues, "latitude")
        longitude = number(text(point, "longitude"), issues, "longitude")
    described = item.find("supplementaryPositionalDescription")
    if described is None:
        described = NONE
    return dict(
        kind=(item.get(XSI_TYPE) or "").split(":")[-1],
        latitude=latitude,
        longitude=longitude,
        length_m=number(text(described, "lengthAffected"), issues, "length_affected"),
        road_number=text(described, "roadInformation/roadNumber"),
        road_name=text(described, "roadInformation/roadName"),
        county_code=text(described, "namedArea/subdivisionCode"),
        county=value_text(described, "namedArea/areaName"),
        description=value_text(described, "locationDescription"),
        path=path,
    )


def parse_period(period, issues):
    days = period.findall("recurringDayWeekMonthPeriod/applicableDay")
    return dict(
        start_at=timestamp(text(period, "startOfPeriod"), issues, "period_start"),
        end_at=timestamp(text(period, "endOfPeriod"), issues, "period_end"),
        time_from=text(period, "recurringTimePeriodOfDay/startTimeOfPeriod"),
        time_to=text(period, "recurringTimePeriodOfDay/endTimeOfPeriod"),
        days=[(e.text or "").strip() for e in days],
    )


def parse_record(situation, record, base, source_day):
    """One situation record: what, how severe, when it applies during source_day, and where."""
    issues = list(base["dq_issue"])

    # When
    validity = record.find("validity/validityTimeSpecification")
    if validity is None:
        validity = NONE
    start = timestamp(text(validity, "overallStartTime"), issues, "start_at")
    end = timestamp(text(validity, "overallEndTime"), issues, "end_at")
    periods = [parse_period(p, issues) for p in validity.findall("validPeriod")]
    try:
        intervals = active_intervals(start, end, periods, source_day)
    except (ValueError, TypeError):
        issues.append("invalid_recurring_period")
        intervals = []

    # Where
    reference = record.find("locationReference")
    if reference is None:
        items = []
    else:
        items = reference.findall("locationContainedInGroup") or [reference]
    locations = [loc for loc in (parse_location(item, issues) for item in items) if loc]

    # What: e.g. roadClosed, temporaryTrafficLights, roadworks.
    subtypes = [
        (child.text or "").strip()
        for child in record
        if child.tag.endswith("Type") and child.text
    ]
    cause = record.find("cause/detailedCauseType")
    detailed_cause = (cause[0].text or "").strip() if cause is not None and len(cause) else None
    comments = {
        text(c, "commentType"): value_text(c, "comment")
        for c in record.findall("generalPublicComment")
    }
    lanes = text(record, "impact/numberOfLanesRestricted")

    return dict(
        base,
        kind="record",
        dq_issue=issues,
        record_id=record.get("id"),
        version=record.get("version"),
        situation_id=situation.get("id"),
        overall_severity=text(situation, "overallSeverity"),
        record_type=(record.get(XSI_TYPE) or "").split(":")[-1],
        record_subtypes=subtypes,
        vehicle_usage=text(record, "vehicleUsage"),
        created_at=timestamp(text(record, "situationRecordCreationTime"), issues, "created_at"),
        version_at=timestamp(text(record, "situationRecordVersionTime"), issues, "version_at"),
        severity=text(record, "severity"),
        probability=text(record, "probabilityOfOccurrence"),
        safety_related=text(record, "safetyRelatedMessage") == "true",
        compliance=text(record, "complianceOption"),
        lanes_restricted=number(lanes, issues, "lanes_restricted", integer=True),
        detailed_cause=detailed_cause,
        roadworks_id=text(record, "roadworksIdentifier"),
        maintenance_type=text(record, "roadMaintenanceType"),
        start_at=start,
        end_at=end,
        valid_periods=periods,
        active_intervals=intervals,
        comment=comments.get("dataProcessingNote"),
        description=comments.get("description"),
        locations=locations,
    )


def parse_situation(payload, base, source_day):
    """A sighting per record per poll; the full record only the first time the task sees it.

    A day is ~690,000 sightings of ~3,000 versions, and the geometry is the bulk.
    """
    seen = base.pop("_seen")
    publication = None
    for _, elem in ET.iterparse(io.BytesIO(payload.encode("utf-8")), events=("end",)):
        name = local(elem.tag)
        if name == "publicationTime" and publication is None:
            publication = timestamp(elem.text, base["dq_issue"], "publication_at")
            continue
        if name != "situation":
            continue

        strip_ns(elem)
        for record in elem.findall("situationRecord"):
            # (id, version) is not unique: ~1.5 % repeat it for a second measure at the same
            # place (e.g. lights + manual direction). Their version times differ.
            version_at = text(record, "situationRecordVersionTime")
            key = (record.get("id"), record.get("version"), version_at)
            if key not in seen:
                seen.add(key)
                full = parse_record(elem, record, base, source_day)
                yield dict(full, publication_at=publication)
            yield dict(
                base,
                kind="sighting",
                record_id=key[0],
                version=key[1],
                publication_at=publication,
                version_at=timestamp(version_at, [], "version_at"),
            )
        elem.clear()


# --- Measurements and their sites ---


def publication_time(root, base):
    return timestamp(text(root, "payload/publicationTime"), base["dq_issue"], "publication_at")


def parse_traveltime(payload, base):
    """One row per road section and 5-minute period."""
    # A section where no vehicle was measured keeps its row: status 'unknown', no travel time.
    root = strip_ns(ET.fromstring(payload))
    publication = publication_time(root, base)
    measured, status = {}, {}
    period = "measurementOrCalculationTime/period/"
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
            start = text(data, period + "startOfPeriod")
            end = text(data, period + "endOfPeriod")
            measured[section] = dict(
                base,
                kind="value",
                dq_issue=issues,
                section_id=section,
                section_version=ref.get("version"),
                publication_at=publication,
                period_start_at=timestamp(start, issues, "period_start"),
                period_end_at=timestamp(end, issues, "period_end"),
                travel_time_s=number(text(data, "travelTime/duration"), issues, "travel_time"),
                free_flow_s=number(text(data, "freeFlowTravelTime/duration"), issues, "free_flow"),
                free_flow_kmh=number(text(data, "freeFlowSpeed/speed"), issues, "free_flow_speed"),
                trend=text(data, "travelTimeTrendType"),
                time_type=text(data, "travelTimeType"),
            )
    for section, row in measured.items():
        yield dict(row, traffic_status=status.get(section))


def weather_quantity(data):
    """(quantity element name, its one leaf value) of one basicData."""
    # Some carry their own measurement time as a sibling; the site's default time covers them.
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
    publication = publication_time(root, base)
    for site in root.iter("siteMeasurements"):
        issues = list(base["dq_issue"])
        ref = site.find("measurementSiteReference")
        measured_at = text(site, "measurementTimeDefault/timeValue")
        row = dict(
            base,
            kind="value",
            dq_issue=issues,
            site_id=ref.get("id") if ref is not None else None,
            site_version=ref.get("version") if ref is not None else None,
            publication_at=publication,
            measurement_at=timestamp(measured_at, issues, "measurement_at"),
        )

        # The lowest index of a quantity is the main sensor (801; 802/803 are further lanes).
        def index(q):
            return number(q.get("index"), issues, "quantity_index", integer=True) or 0

        taken = set()
        for quantity in sorted(site.findall("physicalQuantity"), key=index):
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
                is_text = column == "road_condition"
                row[column] = value if is_text else number(value, issues, column)
        yield row


def parse_weather_sites(payload, base):
    """The weather stations: one row per site, with its position and road."""
    root = strip_ns(ET.fromstring(payload))
    publication = publication_time(root, base)
    point = "pointByCoordinates/pointCoordinates/"
    described = "supplementaryPositionalDescription/"
    for site in root.iter("measurementSite"):
        issues = list(base["dq_issue"])
        location = site.find("measurementSiteLocation")
        if location is None:
            location = NONE
        yield dict(
            base,
            kind="site",
            dq_issue=issues,
            publication_at=publication,
            id=site.get("id"),
            version=site.get("version"),
            name=value_text(site, "measurementSiteName"),
            latitude=number(text(location, point + "latitude"), issues, "latitude"),
            longitude=number(text(location, point + "longitude"), issues, "longitude"),
            road_number=text(location, described + "roadInformation/roadNumber"),
            county_code=text(location, described + "namedArea/subdivisionCode"),
            county=value_text(location, described + "namedArea/areaName"),
        )


def parse_sections(payload, base):
    """The travel time sections: one row per road section, with its path."""
    root = strip_ns(ET.fromstring(payload))
    publication = publication_time(root, base)
    for section in root.iter("predefinedLocationReference"):
        if not len(section):
            continue
        issues = list(base["dq_issue"])
        poslist = text(section, "location/gmlLineString/posList")
        yield dict(
            base,
            kind="section",
            dq_issue=issues,
            publication_at=publication,
            id=section.get("id"),
            version=section.get("version"),
            name=value_text(section, "predefinedLocationName"),
            path=path_from_poslist(poslist, issues),
        )


# --- Points for the spatial joins (mapInArrow bodies) ---


def situation_points(rows):
    """One row per point of each situation location."""
    for row in rows:
        for location in row["locations"] or []:
            points = along_path(location["path"])
            has_point = location["latitude"] is not None and location["longitude"] is not None
            if not points and has_point:
                points = [dict(latitude=location["latitude"], longitude=location["longitude"])]
            for point in points:
                yield dict(
                    record_id=row["record_id"],
                    version=row["version"],
                    version_at=row["version_at"],
                    **point,
                )


def section_points(rows):
    """Points along each travel time section."""
    for row in rows:
        for point in along_path(row["path"]):
            yield dict(section_id=row["id"], **point)


# --- Running the decoders in Spark ---


def failed_delivery(payload):
    """NPRA's own answer that a poll failed (e.g. "Delivery break"), or None."""
    root = strip_ns(ET.fromstring(payload))
    info = root.find("exchangeInformation/dynamicInformation/returnInformation")
    status = info.find("returnStatus") if info is not None else None
    if status is None or status.get("_extendedValue") != "fail":
        return None
    return value_text(info, "returnStatusReason") or "fail"


SNAPSHOT_PARSERS = {
    "traveltime": parse_traveltime,
    "weather": parse_weather,
    "weather-sites": parse_weather_sites,
    "traveltime-locations": parse_sections,
}


def decode_partition(rows, feed):
    """One task's Bronze polls of a feed -> decoded rows."""
    # Situation versions this task has seen, or for other feeds: snapshot hashes.
    seen = set()
    for source in rows:
        issues = []
        base = dict(
            kind=None,
            received_at=timestamp(source["received_at"], issues, "received_at"),
            source_file=source["_bronze_source_file"],
            payload_sha256=source["_bronze_payload_sha256"],
            bronze_run_id=source["_bronze_run_id"],
            dq_issue=issues,
        )
        status = source["_bronze_status"]

        # A fetch error is a missed poll, not bad data: Bronze's run record counts them.
        if status == "fetch_error":
            continue
        if status not in ("ok", "not_modified"):
            yield reject(base, "bronze_" + status)
            continue
        if status == "not_modified" or source["payload"] is None:
            continue

        try:
            if feed == "situation":
                source_day = date.fromisoformat(source["source_date"])
                decoded = parse_situation(source["payload"], dict(base, _seen=seen), source_day)
            elif source["_bronze_payload_sha256"] in seen:
                # We poll at twice NPRA's cadence: most snapshots repeat byte for byte.
                continue
            else:
                seen.add(source["_bronze_payload_sha256"])
                decoded = SNAPSHOT_PARSERS[feed](source["payload"], base)

            empty = True
            for row in decoded:
                empty = False
                yield row
            if not empty:
                continue

            # Well-formed XML without the feed's content. NPRA's own "failed" answer is a missed
            # poll (1 in ~12,000); anything else is a contract failure.
            reason = failed_delivery(source["payload"])
            if reason:
                yield reject(base, "source_delivery_failed", {"reason": reason})
            else:
                yield reject(base, "unknown_message_shape")
        except ET.ParseError:
            yield reject(base, "malformed_payload")


def schema(feed):
    """The decoded rows' Spark schema per feed. Raw payloads are never part of it."""
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    def struct(**columns):
        return StructType([StructField(k, v) for k, v in columns.items()])

    def all_of(kind, *names):
        return {name: kind for name in names}

    text, time, number = StringType(), TimestampType(), DoubleType()
    texts = ArrayType(text)
    point = struct(latitude=number, longitude=number)
    fields = dict(
        kind=text,
        received_at=time,
        publication_at=time,
        source_file=text,
        payload_sha256=text,
        bronze_run_id=text,
        dq_issue=texts,
        entity_json=text,
    )

    if feed == "situation":
        fields.update(all_of(
            text, "record_id", "version", "situation_id", "overall_severity", "record_type",
            "severity", "probability", "compliance", "vehicle_usage", "detailed_cause",
            "roadworks_id", "maintenance_type", "comment", "description",
        ))
        fields.update(all_of(time, "created_at", "version_at", "start_at", "end_at"))
        period = struct(
            start_at=time, end_at=time, time_from=text, time_to=text, days=texts
        )
        location = struct(
            kind=text, latitude=number, longitude=number, length_m=number, road_number=text,
            road_name=text, county_code=text, county=text, description=text,
            path=ArrayType(point),
        )
        fields.update(
            safety_related=BooleanType(),
            lanes_restricted=IntegerType(),
            record_subtypes=texts,
            valid_periods=ArrayType(period),
            active_intervals=ArrayType(struct(start_at=time, end_at=time)),
            locations=ArrayType(location),
        )

    elif feed == "traveltime":
        fields.update(
            section_id=text,
            section_version=text,
            period_start_at=time,
            period_end_at=time,
            travel_time_s=number,
            free_flow_s=number,
            free_flow_kmh=number,
            trend=text,
            time_type=text,
            traffic_status=text,
        )

    elif feed == "weather":
        fields.update(site_id=text, site_version=text, measurement_at=time, road_condition=text)
        quantities = [c for c in WEATHER_QUANTITIES.values() if c != "road_condition"]
        fields.update(all_of(number, *quantities))

    else:
        fields.update(all_of(text, "id", "version", "name"))
        if feed == "weather-sites":
            fields.update(
                latitude=number,
                longitude=number,
                road_number=text,
                county_code=text,
                county=text,
            )
        else:
            fields.update(path=ArrayType(point))
    return struct(**fields)
