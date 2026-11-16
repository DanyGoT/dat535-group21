"""Entur decoders that run inside Spark tasks. Plain Python: no network, no Spark.

Each decoder yields narrow records with typed UTC times. A bad value becomes a dq_issue;
a malformed message becomes a quarantine record.
"""

import hashlib
import json
import math
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

OSLO = ZoneInfo("Europe/Oslo")

# ET messages are at most 9.5 s apart, even at night (2026-10-29). A bigger jump means
# another task read the messages in between.
INPUT_GAP = timedelta(seconds=60)

SIRI_NAMESPACE = ' xmlns="http://www.siri.org.uk/siri"'

# The operating day inside a dated id, e.g. VYG:DatedServiceJourney:1614_DAL-DRM_26-11-05,
# ..._2026-10-30_ExtraJourney, or Bane NOR's 660:2026-11-05.
DATE_IN_ID = re.compile(r"[_:](\d{4}|\d{2})-(\d{2})-(\d{2})(?:_[A-Za-z]+)?$")

# Journey children that are not part of the journey's own state.
NOT_JOURNEY_STATE = ("RecordedAtTime", "RecordedCalls", "EstimatedCalls")

JOURNEY_FLAGS = (
    ("journey_cancelled", "Cancellation"),
    ("extra_journey", "ExtraJourney"),
    ("monitored", "Monitored"),
)

CALL_TIMES = (
    ("aimed_arrival_at", "AimedArrivalTime"),
    ("aimed_departure_at", "AimedDepartureTime"),
    ("expected_arrival_at", "ExpectedArrivalTime"),
    ("expected_departure_at", "ExpectedDepartureTime"),
    ("actual_arrival_at", "ActualArrivalTime"),
    ("actual_departure_at", "ActualDepartureTime"),
)

# Subscription and heartbeat messages: valid, just no data.
CONTROL_MESSAGES = {
    "HeartbeatNotification",
    "SubscriptionResponse",
    "CheckStatusResponse",
    "TerminateSubscriptionResponse",
    "SubscriptionTerminatedNotification",
}

# What an empty delivery may contain. Anything else is an unknown message shape.
DELIVERY_FIELDS = {
    "ResponseTimestamp", "RequestMessageRef", "SubscriberRef", "SubscriptionRef", "Status",
    "ErrorCondition", "MoreData", "ValidUntil", "Extensions", "EstimatedJourneyVersionFrame",
    "Situations",
}

SX_SCOPES = {"LineRef", "StopPointRef", "StopPlaceRef", "VehicleJourneyRef", "OperatorRef"}
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


# --- Small value parsers. Each appends "invalid_<field>" to issues instead of raising. ---


def local(tag):
    return tag.rsplit("}", 1)[-1]


def strip_ns(root):
    for elem in root.iter():
        elem.tag = local(elem.tag)
    return root


def text(elem, path):
    value = elem.findtext(path)
    if value is None:
        return None
    return value.strip() or None


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
        fits_int = -(2**31) <= result < 2**31
        if not math.isfinite(result) or (integer and not fits_int):
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


# --- Shared record parts ---


def provenance(row):
    """The source columns every decoded record carries."""
    issues = []
    payload_sha = row["_bronze_payload_sha256"]
    return dict(
        received_at=timestamp(row["received_at"], issues, "received_at"),
        source_file=row["_bronze_source_file"],
        payload_sha256=payload_sha,
        record_sha256=row["record_sha256"] if "record_sha256" in row else payload_sha,
        bronze_run_id=row["_bronze_run_id"],
        source_date=str(row["source_date"]),
        dq_issue=issues,
    )


def reject(base, issue, entity=None):
    entity_json = json.dumps(entity, default=str, sort_keys=True) if entity else None
    issues = sorted(set(base["dq_issue"] + [issue]))
    return dict(base, kind="quarantine", dq_issue=issues, entity_json=entity_json)


def siri_root(payload):
    """The parsed message with plain tag names."""
    # Removing the default namespace as text is 5x cheaper than strip_ns() on ET messages.
    root = ET.fromstring(payload.replace(SIRI_NAMESPACE, "", 1))
    return strip_ns(root) if root.tag.startswith("{") else root


def protocol_or_reject(root, base, delivery):
    """A message without data: a control record if it is a known envelope, else quarantine."""
    tags = {e.tag for e in root.iter()}
    if tags & CONTROL_MESSAGES:
        return dict(base, kind="control")
    if delivery not in tags:
        return reject(base, "unknown_message_shape")

    deliveries = list(root.iter(delivery))
    if "false" in [text(e, "Status") for e in deliveries]:
        return reject(base, "source_delivery_error")
    for node in deliveries:
        if any(child.tag not in DELIVERY_FIELDS for child in node):
            return reject(base, "unknown_message_shape")
    for node in root.iter("Situations"):
        if len(node):
            return reject(base, "unknown_message_shape")
    frame_fields = {"RecordedAtTime", "ValidUntil", "Version"}
    for node in root.iter("EstimatedJourneyVersionFrame"):
        if any(child.tag not in frame_fields for child in node):
            return reject(base, "unknown_message_shape")
    return dict(base, kind="control")


# --- ET: estimated timetables ---


def date_in_id(journey_id):
    match = DATE_IN_ID.search(journey_id) if journey_id else None
    if not match:
        return None
    year, month, day_of_month = match.groups()
    full_year = int(year) + (2000 if len(year) == 2 else 0)
    try:
        return date(full_year, int(month), int(day_of_month))
    except ValueError:
        return None


def first_call_day(journey):
    """Oslo date of the first aimed time: last resort for a journey that names no day."""
    # Flagged in Silver: a night journey can belong to the day before.
    for call in journey.iter():
        if call.tag not in ("RecordedCall", "EstimatedCall"):
            continue
        for tag in ("AimedDepartureTime", "AimedArrivalTime"):
            value = call.findtext(tag)
            if not value:
                continue
            try:
                aimed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
                return aimed.astimezone(OSLO).date()
            except (ValueError, OverflowError):
                return None
        return None
    return None


def says_something_new(last, key, state, recorded, actuals=0):
    """Whether this version deserves a row. `last` remembers each entity's state and newest copy.

    actuals: 1 = has an actual arrival, 2 = has an actual departure.
    """
    seen = last.get(key)
    if seen is None:
        last[key] = (state, recorded, actuals)
        return True
    previous, newest, newest_actuals = seen

    # A late replay: older than a copy already seen, and adds no actual time.
    # (On 2026-10-29, 621 such replays became predictions before this check.)
    older = recorded is not None and newest is not None and recorded < newest
    if older and (actuals & ~newest_actuals) == 0:
        return False

    # Keep the first copy with a recorded time, even if it repeats: undated copies can't be placed.
    first_dated = recorded is not None and newest is None
    if recorded is not None and (newest is None or recorded >= newest):
        newest, newest_actuals = recorded, actuals
    last[key] = (state, newest, newest_actuals)
    return previous != state or first_dated


def parse_et(payload, base, last=None):
    """Journey and call versions that say something new. Never invents actual times.

    A journey restates all its calls on every change: in a real hour 54 % of call versions and
    96 % of journey versions were repeats. States are kept as hashes (~90 MB per task).
    """
    last = {} if last is None else last
    root = siri_root(payload)
    journeys = list(root.iter("EstimatedVehicleJourney"))
    if not journeys:
        yield protocol_or_reject(root, base, "EstimatedTimetableDelivery")
        return

    for index, journey in enumerate(journeys):
        issues = list(base["dq_issue"])

        # Identity: the framed ref (id + day), else the direct ref, else an extra journey's code.
        framed = journey.find("FramedVehicleJourneyRef")
        jid, op = None, None
        if framed is not None:
            jid = text(framed, "DatedVehicleJourneyRef")
            op = day(text(framed, "DataFrameRef"), issues)
        direct = text(journey, "DatedVehicleJourneyRef")
        extra = text(journey, "EstimatedVehicleJourneyCode")
        jid = jid or direct or extra

        # No explicit day: guess from the id, then the first call. Silver decides which wins.
        derived, source = None, None
        if op is None:
            derived, source = date_in_id(jid), "id"
            if derived is None:
                derived, source = first_call_day(journey), "first_call"

        recorded_issues = []
        recorded = timestamp(text(journey, "RecordedAtTime"), recorded_issues, "recorded_at")
        issues += recorded_issues

        shared = dict(
            received_at=base["received_at"],
            source_file=base["source_file"],
            payload_sha256=base["payload_sha256"],
            bronze_run_id=base["bronze_run_id"],
            source_date=base["source_date"],
            journey_id=jid,
            operating_date=op,
            dated_journey_id=direct,
            derived_operating_date=derived,
            derived_date_source=source if derived else None,
        )

        # The journey row.
        state = hash(tuple(
            (child.tag, child.text) for child in journey if child.tag not in NOT_JOURNEY_STATE
        ))
        if says_something_new(last, (op, jid), state, recorded):
            mode = text(journey, "VehicleMode")
            ji = list(issues)
            flags = {field: boolean(text(journey, tag), ji, field) for field, tag in JOURNEY_FLAGS}
            codespace = text(journey, "DataSource") or (jid.split(":")[0] if jid else None)
            yield dict(
                shared,
                kind="journey",
                dq_issue=ji,
                entity_index=index,
                record_sha256=base.get("record_sha256"),
                recorded_at=recorded,
                line_id=text(journey, "LineRef"),
                operator_id=text(journey, "OperatorRef"),
                direction_id=text(journey, "DirectionRef"),
                mode=canonical_mode(mode),
                source_mode=mode,
                codespace=codespace,
                vehicle_id=text(journey, "VehicleRef"),
                **flags,
            )

        # The call rows. They carry no journey attributes; Silver joins them from the journey.
        calls = list(journey.iter("RecordedCall")) + list(journey.iter("EstimatedCall"))
        for call_index, call in enumerate(calls):
            raw_order = call.findtext("Order")
            state = hash((call.tag, tuple((child.tag, child.text) for child in call)))
            actuals = 0
            if call.find("ActualArrivalTime") is not None:
                actuals += 1
            if call.find("ActualDepartureTime") is not None:
                actuals += 2
            if not says_something_new(last, (op, jid, raw_order), state, recorded, actuals):
                continue

            ci = list(issues)
            order = raw_order.strip() if raw_order else None
            row = dict(
                shared,
                kind="call",
                dq_issue=ci,
                entity_index=index * 100000 + call_index,
                recorded_at=recorded,
                call_order=number(order, ci, "call_order", integer=True),
                quay_id=text(call, "StopPointRef"),
                call_kind=call.tag,
                call_cancelled=boolean(text(call, "Cancellation"), ci, "call_cancelled"),
                extra_call=boolean(text(call, "ExtraCall"), ci, "extra_call"),
                occupancy=text(call, "Occupancy"),
                arrival_status=text(call, "ArrivalStatus"),
                departure_status=text(call, "DepartureStatus"),
            )
            for field, tag in CALL_TIMES:
                row[field] = timestamp(text(call, tag), ci, field)
            yield row


# --- SX: situations (disruption messages) ---


def parse_sx(payload, base):
    """One row per situation version, with its validity periods and what it affects."""
    root = strip_ns(ET.fromstring(payload))
    situations = list(root.iter("PtSituationElement"))
    if not situations:
        yield protocol_or_reject(root, base, "SituationExchangeDelivery")
        return

    for index, situation in enumerate(situations):
        issues = list(base["dq_issue"])
        periods = [
            dict(
                start_at=timestamp(text(period, "StartTime"), issues, "validity_start"),
                end_at=timestamp(text(period, "EndTime"), issues, "validity_end"),
            )
            for period in situation.findall("ValidityPeriod")
        ]

        scopes = []
        affects = situation.find("Affects")
        if affects is not None:
            for node in affects.iter():
                if node.tag in SX_SCOPES:
                    scopes.append(dict(entity_type=node.tag, entity_id=(node.text or "").strip()))

        def translated(tag):
            return [
                dict(language=e.get(XML_LANG) or e.get("lang"), text=(e.text or "").strip())
                for e in situation.findall(tag)
            ]

        # Sampled RUT messages have no native Version: a content hash keeps revisions apart.
        native = text(situation, "Version") or situation.get("version")
        if native:
            version = "native:" + native
        else:
            xml = ET.tostring(situation, encoding="unicode")
            version = "sha256:" + digest(ET.canonicalize(xml, strip_text=True))

        yield dict(
            base,
            kind="sx",
            entity_index=index,
            dq_issue=issues,
            participant_id=text(situation, "ParticipantRef"),
            situation_id=text(situation, "SituationNumber"),
            native_version=native,
            version_key=version,
            created_at=timestamp(text(situation, "CreationTime"), issues, "created_at"),
            progress=text(situation, "Progress"),
            severity=text(situation, "Severity"),
            report_type=text(situation, "ReportType"),
            validity_periods=periods,
            affects=scopes,
            summaries=translated("Summary"),
            descriptions=translated("Description"),
        )


# --- Vehicle positions (GraphQL over websocket) ---


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
    # Some data and some errors: keep the data, quarantine the fact.
    if body.get("errors"):
        yield reject(base, "graphql_partial_error")

    for index, vehicle in enumerate(vehicles):
        if not isinstance(vehicle, dict):
            yield reject(base, "invalid_vehicle_shape")
            continue
        issues = list(base["dq_issue"])

        def nested(name, key):
            return (vehicle.get(name) or {}).get(key)

        row = dict(
            base,
            kind="vehicle",
            entity_index=index,
            dq_issue=issues,
            vehicle_id=vehicle.get("vehicleId"),
            codespace=nested("codespace", "codespaceId"),
            journey_id=nested("serviceJourney", "id"),
            operating_date=day(nested("serviceJourney", "date"), issues),
            dated_journey_id=nested("datedServiceJourney", "id"),
            line_id=nested("line", "lineRef"),
            operator_id=nested("operator", "operatorRef"),
            direction_name=vehicle.get("direction"),
            mode=canonical_mode(vehicle.get("mode")),
            source_mode=vehicle.get("mode"),
            quay_id=nested("monitoredCall", "stopPointRef"),
            occupancy=vehicle.get("occupancyStatus"),
            vehicle_status=vehicle.get("vehicleStatus"),
        )
        times = (
            ("last_updated_at", vehicle.get("lastUpdated")),
            ("expires_at", vehicle.get("expiration")),
        )
        numbers = (
            ("latitude", nested("location", "latitude")),
            ("longitude", nested("location", "longitude")),
            ("speed", vehicle.get("speed")),
            ("bearing", vehicle.get("bearing")),
            ("delay_s", vehicle.get("delay")),
            ("link_distance_m", nested("progressBetweenStops", "linkDistance")),
            ("progress_pct", nested("progressBetweenStops", "percentage")),
        )
        flags = (
            ("monitored", vehicle.get("monitored")),
            ("in_congestion", vehicle.get("inCongestion")),
            ("vehicle_at_stop", nested("monitoredCall", "vehicleAtStop")),
        )
        for field, value in times:
            row[field] = timestamp(value, issues, field)
        for field, value in numbers:
            row[field] = number(value, issues, field)
        call_order = nested("monitoredCall", "order")
        row["call_order"] = number(call_order, issues, "call_order", integer=True)
        for field, value in flags:
            row[field] = boolean(value, issues, field)
        yield row


# --- Running the decoders in Spark ---


def decode_partition(rows, feed):
    """Decode one task's Bronze rows."""
    # parse_et's memory only holds for an unbroken run of messages, but Spark can hand a task
    # several pieces of different hours. Start over wherever the input jumps (INPUT_GAP).
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
            elif feed == "sx":
                yield from parse_sx(source["payload"], base)
            else:
                yield from parse_vehicles(source["payload"], base)
        except (ET.ParseError, json.JSONDecodeError):
            yield reject(base, "malformed_payload")


def arrow_decoder(decode, spark_schema, flush_rows=20000):
    """A mapInArrow function: Bronze rows in, decoded rows out, one decode() call per task."""
    # Arrow instead of createDataFrame: pickling every row cost 60 % of the decode time
    # (docs/report/evidence/silver-decode-cpu-profile-2026-11-04.json).

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
    """The decoded rows' Spark schema per feed. Raw payloads are never part of it."""
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    def all_of(kind, *names):
        return {name: kind for name in names}

    def struct(**fields):
        return StructType([StructField(k, v) for k, v in fields.items()])

    text, time, flag = StringType(), TimestampType(), BooleanType()
    fields = {
        "kind": text,
        "received_at": time,
        "source_file": text,
        "payload_sha256": text,
        "record_sha256": text,
        "bronze_run_id": text,
        "source_date": text,
        "entity_index": LongType(),
        "dq_issue": ArrayType(text, False),
        "entity_json": text,
    }
    if feed in {"et", "vehicles"}:
        fields.update(all_of(
            text, "journey_id", "dated_journey_id", "line_id", "operator_id", "direction_id",
            "direction_name", "mode", "source_mode", "codespace", "vehicle_id", "quay_id",
            "occupancy",
        ))
        fields.update(operating_date=DateType(), call_order=IntegerType(), monitored=flag)

    if feed == "et":
        fields.update(derived_operating_date=DateType(), derived_date_source=text)
        fields.update(all_of(time, "recorded_at", *[field for field, _ in CALL_TIMES]))
        fields.update(all_of(
            flag, "journey_cancelled", "call_cancelled", "extra_journey", "extra_call"
        ))
        fields.update(all_of(text, "call_kind", "arrival_status", "departure_status"))
    elif feed == "vehicles":
        fields.update(all_of(
            DoubleType(), "latitude", "longitude", "speed", "bearing", "delay_s",
            "link_distance_m", "progress_pct",
        ))
        fields.update(all_of(flag, "in_congestion", "vehicle_at_stop"))
        fields.update(last_updated_at=time, expires_at=time, vehicle_status=text)
    else:
        fields.update(all_of(
            text, "participant_id", "situation_id", "native_version", "version_key",
            "progress", "severity", "report_type",
        ))
        fields["created_at"] = time
        fields["validity_periods"] = ArrayType(struct(start_at=time, end_at=time))
        fields["affects"] = ArrayType(struct(entity_type=text, entity_id=text))
        fields["summaries"] = ArrayType(struct(language=text, text=text))
        fields["descriptions"] = ArrayType(struct(language=text, text=text))
    return struct(**fields)
