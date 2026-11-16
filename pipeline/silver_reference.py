"""Read the archived NeTEx (plan) and NSR (stops) zips inside Spark tasks.

One task per line member, streamed. Shared dictionaries are cached per worker; the national
timetable is never collected on the driver or extracted to disk.
"""

import zipfile
import xml.etree.ElementTree as ET
from collections import ChainMap, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path

from silver_parse import OSLO, boolean, canonical_mode, local, number, ref, strip_ns, text

# The NeTEx elements a ServiceJourney refers to.
INDEX_TAGS = {
    "OperatingDay", "OperatingPeriod", "DayType", "DayTypeAssignment",
    "PassengerStopAssignment", "JourneyPattern", "Line", "Route", "Operator",
}

DAY_TYPE_PARTS = {"properties", "PropertyOfDay", "DaysOfWeek", "Name", "Description"}
WEEKDAY_WORDS = {
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "Everyday", "Weekdays", "Weekend", "None",
}

# Planned call times: (column, NeTEx prefix of <prefix>Time and <prefix>DayOffset).
PASSING_TIMES = (
    ("planned_arrival_at", "Arrival"),
    ("planned_departure_at", "Departure"),
    ("earliest_departure_at", "EarliestDeparture"),
    ("latest_arrival_at", "LatestArrival"),
)


def archived_file(landing, dataset, day):
    """The newest snapshot on or before the operating day (never a later one)."""
    candidates = []
    for folder in (Path(landing) / dataset).glob("date=*"):
        snapshot = date.fromisoformat(folder.name.removeprefix("date="))
        if snapshot <= day:
            candidates.extend((snapshot, p) for p in folder.glob("*.zip"))
    if not candidates:
        raise FileNotFoundError(f"no archived {dataset} snapshot on/before {day}")
    newest = max(d for d, _ in candidates)
    files = [p for d, p in candidates if d == newest]
    if len(files) != 1:
        raise ValueError(f"ambiguous {dataset} snapshot {newest}: {files}")
    return newest, files[0]


def elements(archive, member, tags):
    """Stream the subtrees with the given tags; freed from memory once yielded."""
    with zipfile.ZipFile(archive) as z, z.open(member) as source:
        stack, capture = [], None
        for event, elem in ET.iterparse(source, events=("start", "end")):
            if event == "start":
                stack.append(elem)
                if capture is None and local(elem.tag) in tags:
                    capture = len(stack)
                continue
            if capture == len(stack):
                yield strip_ns(elem)
                capture = None
            # Outside a captured subtree: drop the element from its parent to free memory.
            if capture is None:
                if len(stack) > 1:
                    stack[-2].remove(elem)
                elem.clear()
            stack.pop()


def index_entry(elem, kind, ident):
    """The few values of one indexed element that journeys need, as a small dict."""
    data = {"id": ident, "version": elem.get("version"), "name": text(elem, "Name")}
    if kind == "OperatingDay":
        data["date"] = text(elem, "CalendarDate")

    elif kind == "OperatingPeriod":
        data.update(
            start=text(elem, "FromDate"),
            end=text(elem, "ToDate"),
            start_ref=ref(elem, "FromOperatingDayRef"),
            end_ref=ref(elem, "ToOperatingDayRef"),
        )

    elif kind == "DayType":
        days = elem.findall("properties/PropertyOfDay/DaysOfWeek")
        data["weekdays"] = [word for e in days for word in (e.text or "").split()]
        children = (e for e in elem.iter() if e is not elem)
        data["unsupported"] = any(local(e.tag) not in DAY_TYPE_PARTS for e in children)

    elif kind == "DayTypeAssignment":
        availability = text(elem, "isAvailable")
        invalid = []
        if availability is None:
            available = True
        else:
            available = boolean(availability, invalid, "isAvailable")
        if invalid:
            raise ValueError(f"invalid availability in DayTypeAssignment {ident}")
        data.update(
            day_type=ref(elem, "DayTypeRef"),
            date=text(elem, "Date"),
            day_ref=ref(elem, "OperatingDayRef"),
            period=ref(elem, "OperatingPeriodRef"),
            available=available,
        )

    elif kind == "PassengerStopAssignment":
        data.update(stop=ref(elem, "ScheduledStopPointRef"), quay=ref(elem, "QuayRef"))

    elif kind == "JourneyPattern":
        data.update(route=ref(elem, "RouteRef"), direction=text(elem, "DirectionType"), points={})
        for point in elem.findall("pointsInSequence/StopPointInJourneyPattern"):
            issues = []
            data["points"][point.get("id")] = dict(
                order=number(point.get("order"), issues, "order", integer=True),
                stop=ref(point, "ScheduledStopPointRef"),
                boarding=boolean(text(point, "ForBoarding"), issues, "boarding"),
                alighting=boolean(text(point, "ForAlighting"), issues, "alighting"),
                issues=issues,
            )

    elif kind == "Line":
        data.update(
            operator_id=ref(elem, "OperatorRef"),
            mode=canonical_mode(text(elem, "TransportMode")),
            public_code=text(elem, "PublicCode"),
            is_flexible=elem.tag == "FlexibleLine",
        )

    elif kind == "Route":
        data["direction"] = text(elem, "DirectionType")
        data["line_id"] = ref(elem, "LineRef") or ref(elem, "FlexibleLineRef")
    return data


def index_member(archive, member):
    """The elements of one zip member that journeys refer to, by kind and id."""
    index = {k: {} for k in INDEX_TAGS}
    index["dated"] = defaultdict(list)
    index["dated_services"] = set()
    for elem in elements(archive, member, INDEX_TAGS | {"DatedServiceJourney", "FlexibleLine"}):
        kind = "Line" if elem.tag == "FlexibleLine" else elem.tag
        ident = elem.get("id")

        if kind == "DatedServiceJourney":
            # Its OperatingDayRef is resolved once the whole member is read.
            sj = ref(elem, "ServiceJourneyRef")
            index["dated_services"].add(sj)
            day_ref = ref(elem, "OperatingDayRef")
            index["dated"][sj].append((ident, day_ref, text(elem, "ServiceAlteration")))
            continue

        data = index_entry(elem, kind, ident)
        # FIN reuses one assignment id for several DayTypes: key by the whole rule instead.
        key = ident
        if kind == "DayTypeAssignment":
            key = (ident, data["day_type"], data["date"], data["day_ref"], data["period"],
                   data["available"])
        previous = index[kind].get(key)
        if previous and previous != data:
            raise ValueError(f"conflicting {kind} {ident} in {member}")
        index[kind][key] = data
    return index


@lru_cache(maxsize=2)
def shared_index(archive, member):
    """A shared member's index, kept for the next line members of the same task."""
    return index_member(archive, member)


class Calendar:
    """Whether a DayType runs on the operating day.

    Looks in the member's own rules, then its shared file's. `fallbacks` cover ids that only
    another shared file of the same codespace has.
    """

    def __init__(self, indexes, day, fallbacks=()):
        self.day = day
        self.maps = {k: ChainMap(*(i[k] for i in indexes)) for k in INDEX_TAGS}
        self.assignments = defaultdict(list)
        for assignment in self.maps["DayTypeAssignment"].values():
            self.assignments[assignment["day_type"]].append(assignment)
        self.cache = {}
        self.fallbacks = fallbacks

    def operating_day(self, ident):
        record = self.maps["OperatingDay"].get(ident)
        if record:
            return date.fromisoformat(record["date"])
        for fallback in self.fallbacks:
            if ident in fallback.maps["OperatingDay"]:
                return fallback.operating_day(ident)
        raise ValueError(f"unresolved OperatingDay {ident}")

    def weekday_matches(self, weekdays):
        if weekdays - WEEKDAY_WORDS:
            raise ValueError(f"unsupported DaysOfWeek {weekdays - WEEKDAY_WORDS}")
        weekend = self.day.weekday() >= 5
        return (
            not weekdays
            or self.day.strftime("%A") in weekdays
            or "Everyday" in weekdays
            or ("Weekdays" in weekdays and not weekend)
            or ("Weekend" in weekdays and weekend)
        )

    def period_bounds(self, period_id):
        period = self.maps["OperatingPeriod"].get(period_id)
        if period is None:
            raise ValueError(f"unresolved OperatingPeriod {period_id}")
        if period["start"]:
            start = date.fromisoformat(period["start"][:10])
        else:
            start = self.operating_day(period["start_ref"])
        if period["end"]:
            end = date.fromisoformat(period["end"][:10])
        else:
            end = self.operating_day(period["end_ref"])
        return start, end

    def active(self, ident):
        if ident in self.cache:
            return self.cache[ident]

        day_type = self.maps["DayType"].get(ident)
        if day_type is None:
            for fallback in self.fallbacks:
                # Resolve the whole rule in that file: period ids can overlap between exports.
                if ident in fallback.maps["DayType"]:
                    self.cache[ident] = fallback.active(ident)
                    return self.cache[ident]
        if day_type is None or day_type["unsupported"]:
            raise ValueError(f"missing or unsupported DayType {ident}")
        weekday_ok = self.weekday_matches(set(day_type["weekdays"]))

        # An assignment applies on an exact date, or within a period on a matching weekday.
        # The day runs if some assignment includes it and none excludes it.
        included, excluded = False, False
        for a in self.assignments.get(ident, []):
            exact = a["date"]
            if not exact and a["day_ref"]:
                exact = str(self.operating_day(a["day_ref"]))
            if exact:
                applies = date.fromisoformat(exact) == self.day
            elif a["period"]:
                start, end = self.period_bounds(a["period"])
                applies = start <= self.day <= end and weekday_ok
            else:
                raise ValueError(f"unsupported DayTypeAssignment {a['id']}")
            included |= applies and a["available"]
            excluded |= applies and not a["available"]

        self.cache[ident] = included and not excluded
        return self.cache[ident]


def planned_time(value, offset, day, issues, field):
    """Local timetable clock + day offset -> UTC. Flags times a DST change makes unclear."""
    if value is None:
        return None
    try:
        clock = time.fromisoformat(value)
        dt = datetime.combine(day + timedelta(days=int(offset or 0)), clock)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
        # Usually one instant. Autumn's repeated hour has two, spring's skipped hour none.
        instants = {dt.replace(tzinfo=OSLO, fold=fold).astimezone(timezone.utc) for fold in (0, 1)}
        candidates = {t for t in instants if t.astimezone(OSLO).replace(tzinfo=None) == dt}
        if len(candidates) != 1:
            issues.append(f"{'ambiguous' if candidates else 'nonexistent'}_{field}")
            return None
        return candidates.pop()
    except (ValueError, TypeError):
        issues.append(f"invalid_{field}")
        return None


def shared_name(codespace, flexible):
    return f"_{codespace}{'_flexible' if flexible else ''}_shared_data.xml"


def member_calendar(archive, member, shared, day):
    """The member's own index and the Calendar for its day, with the right shared files."""
    own = index_member(archive, member)
    codespace = member.split("_", 1)[0]
    with zipfile.ZipFile(archive) as z:
        names = set(z.namelist())

    # Some flexible members have ordinary filenames: their Line picks the shared file.
    if own["Line"]:
        scopes = {line["is_flexible"] for line in own["Line"].values()}
        if len(scopes) > 1:
            raise ValueError(f"mixed fixed/flexible calendar scopes in {member}")
        preferred = shared_name(codespace, True in scopes)
        if preferred in names:
            shared = preferred

    indexes = [own] + ([shared_index(archive, shared)] if shared else [])
    alternatives = [
        n
        for n in (shared_name(codespace, False), shared_name(codespace, True))
        if n != shared and n != member and n in names
    ]
    fallbacks = [Calendar([own, shared_index(archive, n)], day) for n in alternatives]
    return own, indexes, Calendar(indexes, day, fallbacks)


def plan_member(archive, member, shared, day):
    """The lines, operators, journeys and calls of one line member that run on the day."""
    own, indexes, calendar = member_calendar(archive, member, shared, day)
    maps = calendar.maps

    stop_quays = defaultdict(set)
    for assignment in maps["PassengerStopAssignment"].values():
        if assignment["quay"]:
            stop_quays[assignment["stop"]].add(assignment["quay"])

    for line in own["Line"].values():
        yield dict(kind="line", **line, source_member=member, dq_issue=[])
    for operator in own["Operator"].values():
        yield dict(kind="operator", **operator, source_member=member, dq_issue=[])

    # Dated journeys on this day, per ServiceJourney.
    dated = defaultdict(list)
    for index in indexes:
        for sj, versions in index["dated"].items():
            for ident, day_ref, alteration in versions:
                if calendar.operating_day(day_ref) == day:
                    dated[sj].append((ident, alteration))
    dated_services = set().union(*(i["dated_services"] for i in indexes))

    for sj in elements(archive, member, {"ServiceJourney"}):
        jid = sj.get("id")

        # Runs today: by its dated journeys if it has any, else by its day types.
        day_types = [e.get("ref") for e in sj.findall("dayTypes/DayTypeRef")]
        if jid in dated_services:
            active = bool(dated[jid])
        elif day_types:
            active = any(calendar.active(t) for t in day_types)
        else:
            raise ValueError(f"journey {jid} has neither dated journeys nor supported calendar")
        if not active:
            continue

        issues = []
        pattern_id = ref(sj, "JourneyPatternRef")
        pattern = maps["JourneyPattern"].get(pattern_id)
        if pattern is None:
            raise ValueError(f"unresolved JourneyPattern {pattern_id} for {jid}")
        route = maps["Route"].get(pattern["route"], {})
        line_id = ref(sj, "LineRef") or ref(sj, "FlexibleLineRef") or route.get("line_id")
        line = maps["Line"].get(line_id, {})

        aliases = sorted(set(dated[jid]), key=lambda pair: (pair[0], pair[1] or ""))
        alterations = {a for _, a in aliases if a}
        if len(alterations) > 1:
            issues.append("conflicting_service_alteration")

        direction = text(sj, "DirectionType") or pattern["direction"] or route.get("direction")
        journey = dict(
            kind="journey",
            journey_id=jid,
            operating_date=day,
            line_id=line_id,
            operator_id=ref(sj, "OperatorRef") or line.get("operator_id"),
            mode=canonical_mode(text(sj, "TransportMode")) or line.get("mode"),
            direction_type=direction,
            journey_pattern_id=pattern_id,
            source_member=member,
            is_flexible=line.get("is_flexible", False),
            service_alteration=next(iter(alterations)) if len(alterations) == 1 else None,
            dq_issue=issues,
            dated_journey_ids=[ident for ident, _ in aliases],
        )
        yield journey

        passing = sj.findall("passingTimes/TimetabledPassingTime")
        if not passing:
            raise ValueError(f"unsupported timetable without passing times: {jid}")
        for point in passing:
            pi = list(issues)
            point_id = (
                ref(point, "StopPointInJourneyPatternRef")
                or ref(point, "PointInJourneyPatternRef")
            )
            p = pattern["points"].get(point_id)
            if p is None:
                raise ValueError(f"unresolved stop position {point_id} for {jid}")
            pi.extend(p["issues"])

            quays = stop_quays[p["stop"]]
            if len(quays) != 1:
                pi.append("missing_quay_mapping" if not quays else "ambiguous_quay_mapping")

            times = {
                field: planned_time(
                    text(point, tag + "Time"), text(point, tag + "DayOffset"), day, pi, field
                )
                for field, tag in PASSING_TIMES
            }
            yield dict(
                journey,
                kind="call",
                dq_issue=pi,
                call_order=p["order"],
                quay_id=next(iter(quays)) if len(quays) == 1 else None,
                for_boarding=p["boarding"],
                for_alighting=p["alighting"],
                **times,
            )


def plan_partition(items, archive, day):
    for member, shared in items:
        yield from plan_member(archive, member, shared, day)


def plan_members(archive):
    """[(line member, its shared member or None)]: the work items of the plan decode."""
    with zipfile.ZipFile(archive) as z:
        names = set(z.namelist())
    members = []
    for name in sorted(names):
        if name.startswith("_") or not name.endswith(".xml"):
            continue
        codespace = name.split("_", 1)[0]
        flexible = "FlexibleLine" in name
        # The matching shared file, else the other kind, else none.
        shared = shared_name(codespace, flexible)
        if shared not in names:
            alternative = shared_name(codespace, not flexible)
            shared = alternative if alternative in names else None
        members.append((name, shared))
    return members


def stop_entity(elem, kind, member, parent=None):
    issues = []
    latitude = text(elem, "Centroid/Location/Latitude")
    longitude = text(elem, "Centroid/Location/Longitude")
    return dict(
        kind=kind,
        id=elem.get("id"),
        version=elem.get("version"),
        name=text(elem, "Name"),
        stop_place_id=parent,
        parent_stop_place_id=ref(elem, "ParentSiteRef"),
        latitude=number(latitude, issues, "latitude"),
        longitude=number(longitude, issues, "longitude"),
        source_member=member,
        dq_issue=issues,
    )


def nsr_partition(members, archive):
    """Stop places and their quays from NSR zip members."""
    for member in members:
        for stop in elements(archive, member, {"StopPlace"}):
            yield stop_entity(stop, "stop_place", member)
            for quay in stop.findall("quays/Quay"):
                yield stop_entity(quay, "quay", member, stop.get("id"))


def reference_schema():
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    strings = (
        "kind", "id", "version", "name", "source_member", "public_code", "journey_id",
        "line_id", "operator_id", "direction_type", "mode", "journey_pattern_id",
        "service_alteration", "quay_id", "stop_place_id", "parent_stop_place_id",
    )
    fields = {k: StringType() for k in strings}
    fields.update(
        operating_date=DateType(),
        call_order=IntegerType(),
        planned_arrival_at=TimestampType(),
        planned_departure_at=TimestampType(),
        earliest_departure_at=TimestampType(),
        latest_arrival_at=TimestampType(),
        for_boarding=BooleanType(),
        for_alighting=BooleanType(),
        is_flexible=BooleanType(),
        latitude=DoubleType(),
        longitude=DoubleType(),
        dq_issue=ArrayType(StringType(), False),
        dated_journey_ids=ArrayType(StringType(), False),
    )
    return StructType([StructField(k, v) for k, v in fields.items()])
