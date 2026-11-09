"""Pure, task-side Entur decoders. No network access and no Spark dependency.

Each decoder emits narrow records with typed UTC times and source locators. Bad
values become explicit issues; a malformed message becomes a quarantine record.
"""

import hashlib
import json
import math
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

OSLO = ZoneInfo("Europe/Oslo")
# Consecutive ET messages are at most 9.5 s apart even at night, and the receiver's threads swap
# them by at most 0.4 s (2026-10-29). A larger step, either way, means the next row does not
# follow the previous one: another task read what lies between.
INPUT_GAP = timedelta(seconds=60)
SIRI_NAMESPACE = ' xmlns="http://www.siri.org.uk/siri"'
# The operating day inside a dated id: VYG:DatedServiceJourney:1614_DAL-DRM_26-11-05,
# ..._2026-10-30_ExtraJourney, and Bane NOR's 660:2026-11-05.
DATE_IN_ID = re.compile(r"[_:](\d{4}|\d{2})-(\d{2})-(\d{2})(?:_[A-Za-z]+)?$")


def local(tag):
    return tag.rsplit("}", 1)[-1]


def strip_ns(root):
    for elem in root.iter():
        elem.tag = local(elem.tag)
    return root


def text(elem, path):
    value = elem.findtext(path)
    return value.strip() or None if value is not None else None


def ref(elem, path):
    node = elem.find(path)
    return node.get("ref") if node is not None else None


def timestamp(value, issues, field):
    if value is None:
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError("offset missing")
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, AttributeError, OverflowError):
        issues.append(f"invalid_{field}")
        return None


def day(value, issues, field="operating_date"):
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except (ValueError, TypeError):
        issues.append(f"invalid_{field}")
        return None


def number(value, issues, field, integer=False):
    if value is None:
        return None
    try:
        result = int(value) if integer else float(value)
        if not math.isfinite(result) or (integer and not -(2**31) <= result < 2**31):
            raise ValueError("outside the column's range")
        return result
    except (ValueError, TypeError, OverflowError):
        issues.append(f"invalid_{field}")
        return None


def boolean(value, issues, field):
    if value is None:
        return None
    if value in (True, "true", "1"):
        return True
    if value in (False, "false", "0"):
        return False
    issues.append(f"invalid_{field}")
    return None


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_mode(value):
    value = value.lower() if value else None
    return "water" if value == "ferry" else value


def provenance(row):
    issues = []
    received = timestamp(row["received_at"], issues, "received_at")
    return dict(received_at=received, source_file=row["_bronze_source_file"],
                payload_sha256=row["_bronze_payload_sha256"],
                record_sha256=row["record_sha256"] if "record_sha256" in row else row["_bronze_payload_sha256"],
                bronze_run_id=row["_bronze_run_id"], source_date=str(row["source_date"]),
                dq_issue=issues)


def reject(base, issue, entity=None):
    return dict(base, kind="quarantine", dq_issue=sorted(set(base["dq_issue"] + [issue])),
                entity_json=json.dumps(entity, default=str, sort_keys=True) if entity else None)


def siri_root(payload):
    """Parsed message with plain tag names. Dropping the default namespace before parsing costs a
    fifth of strip_ns() on real ET messages; anything else still goes through strip_ns()."""
    root = ET.fromstring(payload.replace(SIRI_NAMESPACE, "", 1))
    return strip_ns(root) if root.tag.startswith("{") else root


def date_in_id(journey_id):
    match = DATE_IN_ID.search(journey_id) if journey_id else None
    if not match:
        return None
    year, month, day_of_month = match.groups()
    try:
        return date(int(year) + (2000 if len(year) == 2 else 0), int(month), int(day_of_month))
    except ValueError:
        return None


def first_call_day(journey):
    """Local date of the first aimed time: the last resort for an extra journey that names no
    operating day anywhere. Flagged, because a night journey can belong to the day before."""
    for call in journey.iter():
        if call.tag in ("RecordedCall", "EstimatedCall"):
            for tag in ("AimedDepartureTime", "AimedArrivalTime"):
                value = call.findtext(tag)
                if value:
                    try:
                        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).astimezone(OSLO).date()
                    except (ValueError, OverflowError):
                        return None
            return None
    return None


def says_something_new(last, key, state, recorded, actuals=0):
    """Whether a version is worth a row; `last` remembers, per entity, its latest state (a hash)
    and the newest copy seen, even if that copy was dropped as a repeat.

    A late replay of older data is dropped too: older than a copy already seen, with no actual
    time (`actuals`: 1 arrival, 2 departure) that the copy lacks. silver.asof_intervals and
    silver.latest would never pick it, but only while that newer copy exists. Seen on
    2026-10-29: a copy dropped as a repeat let 621 such replays become predictions.

    The first copy with a recorded time is kept even if it repeats: asof_intervals cannot place
    an undated copy, so without it the state would never be known."""
    seen = last.get(key)
    if seen is None:
        last[key] = (state, recorded, actuals)
        return True
    previous, newest, newest_actuals = seen
    if recorded is not None and newest is not None and recorded < newest and (actuals & ~newest_actuals) == 0:
        return False
    first_dated = recorded is not None and newest is None
    if recorded is not None and (newest is None or recorded >= newest):
        newest, newest_actuals = recorded, actuals
    last[key] = (state, newest, newest_actuals)
    return previous != state or first_dated


def parse_et(payload, base, last=None):
    """Journey and call versions, only those that say something new. Never manufacture actuals.

    A journey restates all its calls on every change: in a real hour 54 % of call versions
    and 96 % of journey versions repeat the previous one (98 % of RecordedCalls). `last` is this
    task's memory of each entity (says_something_new), with its state as a hash (~90 MB per
    task, the states themselves would be 560 MB). A dropped repeat loses nothing: a state holds
    until the next one, and the first copy has the earliest availability. A state that repeats
    across tasks stays, which only costs a row. Call rows carry no journey attributes.
    """
    last = {} if last is None else last
    root = siri_root(payload)
    journeys = list(root.iter("EstimatedVehicleJourney"))
    if not journeys:
        yield protocol_or_reject(root, base, "EstimatedTimetableDelivery")
        return
    for index, journey in enumerate(journeys):
        issues = list(base["dq_issue"])
        framed = journey.find("FramedVehicleJourneyRef")
        jid = text(framed, "DatedVehicleJourneyRef") if framed is not None else None
        op = day(text(framed, "DataFrameRef"), issues) if framed is not None else None
        direct = text(journey, "DatedVehicleJourneyRef")
        extra = text(journey, "EstimatedVehicleJourneyCode")
        jid = jid or direct or extra
        # Resolved in Silver: explicit day, then the plan's alias, then these two.
        derived, source = None, None
        if op is None:
            derived, source = date_in_id(jid), "id"
            if derived is None:
                derived, source = first_call_day(journey), "first_call"
        recorded_issues = []
        recorded = timestamp(text(journey, "RecordedAtTime"), recorded_issues, "recorded_at")
        issues += recorded_issues
        shared = dict(received_at=base["received_at"], source_file=base["source_file"], payload_sha256=base["payload_sha256"],
                      bronze_run_id=base["bronze_run_id"], source_date=base["source_date"], journey_id=jid, operating_date=op,
                      dated_journey_id=direct, derived_operating_date=derived, derived_date_source=source if derived else None)
        state = hash(tuple((child.tag, child.text) for child in journey if child.tag not in ("RecordedAtTime", "RecordedCalls", "EstimatedCalls")))
        if says_something_new(last, (op, jid), state, recorded):
            mode = text(journey, "VehicleMode")
            ji = list(issues)
            flags = {field: boolean(text(journey, tag), ji, field) for field, tag in (
                ("journey_cancelled", "Cancellation"), ("extra_journey", "ExtraJourney"), ("monitored", "Monitored"))}
            yield dict(shared, kind="journey", dq_issue=ji, entity_index=index, record_sha256=base.get("record_sha256"),
                       recorded_at=recorded,
                       line_id=text(journey, "LineRef"), operator_id=text(journey, "OperatorRef"),
                       direction_id=text(journey, "DirectionRef"), mode=canonical_mode(mode), source_mode=mode,
                       codespace=text(journey, "DataSource") or (jid.split(":")[0] if jid else None),
                       vehicle_id=text(journey, "VehicleRef"), **flags)
        calls = list(journey.iter("RecordedCall")) + list(journey.iter("EstimatedCall"))
        for call_index, call in enumerate(calls):
            raw_order = call.findtext("Order")
            state = hash((call.tag, tuple((child.tag, child.text) for child in call)))
            actuals = (1 if call.find("ActualArrivalTime") is not None else 0) + (2 if call.find("ActualDepartureTime") is not None else 0)
            if not says_something_new(last, (op, jid, raw_order), state, recorded, actuals):
                continue
            ci = list(issues)
            row = dict(shared, kind="call", dq_issue=ci, entity_index=index * 100000 + call_index,
                       recorded_at=recorded,
                       call_order=number(raw_order.strip() if raw_order else None, ci, "call_order", integer=True),
                       quay_id=text(call, "StopPointRef"), call_kind=call.tag,
                       call_cancelled=boolean(text(call, "Cancellation"), ci, "call_cancelled"),
                       extra_call=boolean(text(call, "ExtraCall"), ci, "extra_call"),
                       occupancy=text(call, "Occupancy"),
                       arrival_status=text(call, "ArrivalStatus"), departure_status=text(call, "DepartureStatus"))
            for field, tag in (("aimed_arrival_at", "AimedArrivalTime"),
                               ("aimed_departure_at", "AimedDepartureTime"),
                               ("expected_arrival_at", "ExpectedArrivalTime"),
                               ("expected_departure_at", "ExpectedDepartureTime"),
                               ("actual_arrival_at", "ActualArrivalTime"),
                               ("actual_departure_at", "ActualDepartureTime")):
                row[field] = timestamp(text(call, tag), ci, field)
            yield row


def protocol_or_reject(root, base, delivery):
    # Empty deliveries and subscription/heartbeat messages are valid evidence,
    # whereas an unfamiliar envelope must be visible as a contract failure.
    controls = {"HeartbeatNotification", "SubscriptionResponse", "CheckStatusResponse",
                "TerminateSubscriptionResponse", "SubscriptionTerminatedNotification"}
    tags = {e.tag for e in root.iter()}
    if tags & controls:
        return dict(base, kind="control")
    if delivery in tags:
        deliveries = list(root.iter(delivery))
        statuses = [text(e, "Status") for e in deliveries]
        if "false" in statuses:
            return reject(base, "source_delivery_error")
        allowed = {"ResponseTimestamp", "RequestMessageRef", "SubscriberRef", "SubscriptionRef", "Status",
                   "ErrorCondition", "MoreData", "ValidUntil", "Extensions", "EstimatedJourneyVersionFrame", "Situations"}
        for node in deliveries:
            if any(child.tag not in allowed for child in node):
                return reject(base, "unknown_message_shape")
        for node in root.iter("Situations"):
            if len(node):
                return reject(base, "unknown_message_shape")
        for node in root.iter("EstimatedJourneyVersionFrame"):
            if any(child.tag not in {"RecordedAtTime", "ValidUntil", "Version"} for child in node):
                return reject(base, "unknown_message_shape")
        return dict(base, kind="control")
    return reject(base, "unknown_message_shape")


def parse_sx(payload, base):
    """One content/native version, with nested validity periods and affected scopes."""
    root = strip_ns(ET.fromstring(payload))
    situations = list(root.iter("PtSituationElement"))
    if not situations:
        yield protocol_or_reject(root, base, "SituationExchangeDelivery")
        return
    for index, situation in enumerate(situations):
        issues = list(base["dq_issue"])
        native = text(situation, "Version") or situation.get("version")
        periods = []
        for period in situation.findall("ValidityPeriod"):
            periods.append(dict(start_at=timestamp(text(period, "StartTime"), issues, "validity_start"),
                                end_at=timestamp(text(period, "EndTime"), issues, "validity_end")))
        affects = situation.find("Affects")
        scopes = []
        if affects is not None:
            for node in affects.iter():
                if node.tag in {"LineRef", "StopPointRef", "StopPlaceRef", "VehicleJourneyRef", "OperatorRef"}:
                    scopes.append(dict(entity_type=node.tag, entity_id=(node.text or "").strip()))
        def translated(tag):
            return [dict(language=e.get("{http://www.w3.org/XML/1998/namespace}lang") or e.get("lang"),
                         text=(e.text or "").strip()) for e in situation.findall(tag)]
        # No native Version in the sampled RUT SX messages. A content hash keeps
        # distinct revisions instead of silently replacing every message with v0.
        version = "native:" + native if native else "sha256:" + digest(ET.canonicalize(ET.tostring(situation, encoding="unicode"), strip_text=True))
        yield dict(base, kind="sx", entity_index=index, dq_issue=issues,
                   participant_id=text(situation, "ParticipantRef"),
                   situation_id=text(situation, "SituationNumber"), native_version=native,
                   version_key=version, created_at=timestamp(text(situation, "CreationTime"), issues, "created_at"),
                   progress=text(situation, "Progress"), severity=text(situation, "Severity"),
                   report_type=text(situation, "ReportType"), validity_periods=periods,
                   affects=scopes, summaries=translated("Summary"), descriptions=translated("Description"))


def parse_vehicles(payload, base):
    message = json.loads(payload)
    if not isinstance(message, dict):
        yield reject(base, "unknown_message_shape")
        return
    if message.get("type") in {"connection_ack", "ping", "pong", "complete"}:
        yield dict(base, kind="control")
        return
    # graphql-transport-ws error frames have an array payload, not a data object.
    if message.get("type") == "error":
        yield reject(base, "graphql_error")
        return
    body = message.get("payload")
    if not isinstance(body, dict):
        yield reject(base, "unknown_message_shape")
        return
    data = body.get("data") or {}
    if not isinstance(data, dict):
        yield reject(base, "unknown_message_shape")
        return
    vehicles = data.get("vehicles")
    if message.get("type") != "next" or not isinstance(vehicles, list):
        yield reject(base, "graphql_error" if body.get("errors") else "unknown_message_shape")
        return
    if body.get("errors"):
        yield reject(base, "graphql_partial_error")
    for index, vehicle in enumerate(vehicles):
        if not isinstance(vehicle, dict):
            yield reject(base, "invalid_vehicle_shape")
            continue
        issues = list(base["dq_issue"])
        def nested(name, key):
            return (vehicle.get(name) or {}).get(key)
        row = dict(base, kind="vehicle", entity_index=index, dq_issue=issues,
                   vehicle_id=vehicle.get("vehicleId"), codespace=nested("codespace", "codespaceId"),
                   journey_id=nested("serviceJourney", "id"),
                   operating_date=day(nested("serviceJourney", "date"), issues),
                   dated_journey_id=nested("datedServiceJourney", "id"),
                   line_id=nested("line", "lineRef"), operator_id=nested("operator", "operatorRef"),
                   direction_name=vehicle.get("direction"), mode=canonical_mode(vehicle.get("mode")), source_mode=vehicle.get("mode"),
                   quay_id=nested("monitoredCall", "stopPointRef"),
                   occupancy=vehicle.get("occupancyStatus"), vehicle_status=vehicle.get("vehicleStatus"))
        for field, value in (("last_updated_at", vehicle.get("lastUpdated")),
                             ("expires_at", vehicle.get("expiration"))):
            row[field] = timestamp(value, issues, field)
        for field, value in (("latitude", nested("location", "latitude")),
                             ("longitude", nested("location", "longitude")),
                             ("speed", vehicle.get("speed")), ("bearing", vehicle.get("bearing")),
                             ("delay_s", vehicle.get("delay")),
                             ("link_distance_m", nested("progressBetweenStops", "linkDistance")),
                             ("progress_pct", nested("progressBetweenStops", "percentage"))):
            row[field] = number(value, issues, field)
        row["call_order"] = number(nested("monitoredCall", "order"), issues, "call_order", integer=True)
        for field, value in (("monitored", vehicle.get("monitored")),
                             ("in_congestion", vehicle.get("inCongestion")),
                             ("vehicle_at_stop", nested("monitoredCall", "vehicleAtStop"))):
            row[field] = boolean(value, issues, field)
        yield row


def decode_partition(rows, feed):
    """One task's Bronze rows. parse_et's memory is only right for an unbroken run of messages,
    but Spark packs pieces of several hours into one task, largest first: on 2026-10-29 a task
    read 03:57 before 01:02, another 15:xx and then 18:47, missing the states in between.
    Memory starts over wherever the input breaks (INPUT_GAP)."""
    last, previous = {}, None
    for source in rows:
        base = provenance(source)
        received = base["received_at"]
        if received is not None:
            if previous is not None and abs(received - previous) > INPUT_GAP:
                last.clear()
            previous = received
        if source["_bronze_status"] != "ok":
            yield reject(base, "bronze_" + source["_bronze_status"])
            continue
        try:
            if feed == "et":
                yield from parse_et(source["payload"], base, last)
            else:
                yield from {"sx": parse_sx, "vehicles": parse_vehicles}[feed](source["payload"], base)
        except (ET.ParseError, json.JSONDecodeError):
            yield reject(base, "malformed_payload")


def arrow_decoder(decode, spark_schema, flush_rows=20000):
    """mapInArrow body: Bronze rows in, decoded rows out, one decode() call per task so it can
    keep state. Pickling every row through createDataFrame cost 60 % of the decode time
    (docs/report/evidence/silver-decode-cpu-profile-2026-11-04.json); Arrow also checks types."""
    def run(batches):
        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        target = to_arrow_schema(spark_schema)
        names = target.names

        def rows():
            for batch in batches:
                yield from batch.to_pylist()

        columns, count = {n: [] for n in names}, 0
        for record in decode(rows()):
            for name in names:
                columns[name].append(record.get(name))
            count += 1
            if count >= flush_rows:
                yield pa.RecordBatch.from_pydict(columns, schema=target)
                columns, count = {n: [] for n in names}, 0
        if count:
            yield pa.RecordBatch.from_pydict(columns, schema=target)
    return run


def schema(feed):
    """Stable, explicit Spark contracts. Raw payloads never enter these schemas."""
    from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType, IntegerType, LongType,
                                   StringType, StructField, StructType, TimestampType)
    fields = {"kind": StringType(), "received_at": TimestampType(), "source_file": StringType(),
              "payload_sha256": StringType(), "record_sha256": StringType(), "bronze_run_id": StringType(), "source_date": StringType(),
              "entity_index": LongType(), "dq_issue": ArrayType(StringType(), False),
              "entity_json": StringType()}
    if feed in {"et", "vehicles"}:
        fields.update({k: StringType() for k in ("journey_id", "dated_journey_id", "line_id", "operator_id",
                                               "direction_id", "direction_name", "mode", "source_mode", "codespace", "vehicle_id", "quay_id", "occupancy")})
        fields.update(operating_date=DateType(), call_order=IntegerType(), monitored=BooleanType())
    if feed == "et":
        fields.update(derived_operating_date=DateType(), derived_date_source=StringType())
        fields.update({k: TimestampType() for k in ("recorded_at", "aimed_arrival_at", "aimed_departure_at",
                                                  "expected_arrival_at", "expected_departure_at",
                                                  "actual_arrival_at", "actual_departure_at")})
        fields.update({k: BooleanType() for k in ("journey_cancelled", "call_cancelled", "extra_journey", "extra_call")})
        fields.update({k: StringType() for k in ("call_kind", "arrival_status", "departure_status")})
    elif feed == "vehicles":
        fields.update({k: DoubleType() for k in ("latitude", "longitude", "speed", "bearing", "delay_s",
                                               "link_distance_m", "progress_pct")})
        fields.update({k: BooleanType() for k in ("in_congestion", "vehicle_at_stop")})
        fields.update(last_updated_at=TimestampType(), expires_at=TimestampType(), vehicle_status=StringType())
    else:
        fields.update({k: StringType() for k in ("participant_id", "situation_id", "native_version", "version_key",
                                               "progress", "severity", "report_type")})
        fields["created_at"] = TimestampType()
        for name, columns in (("validity_periods", {"start_at": TimestampType(), "end_at": TimestampType()}),
                              ("affects", {"entity_type": StringType(), "entity_id": StringType()}),
                              ("summaries", {"language": StringType(), "text": StringType()}),
                              ("descriptions", {"language": StringType(), "text": StringType()})):
            fields[name] = ArrayType(StructType([StructField(k, v) for k, v in columns.items()]))
    return StructType([StructField(k, v) for k, v in fields.items()])
