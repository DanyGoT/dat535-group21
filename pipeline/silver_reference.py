"""Stream the archived NeTEx/NSR zips; resolve references before emitting the plan.

One Spark task handles a line member. Shared dictionaries are cached per worker;
the national timetable is never collected on the driver or extracted to disk.
"""

import zipfile
import xml.etree.ElementTree as ET
from collections import ChainMap, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from silver_parse import boolean, canonical_mode, local, number, ref, strip_ns, text

OSLO = ZoneInfo("Europe/Oslo")
INDEX_TAGS = {"OperatingDay", "OperatingPeriod", "DayType", "DayTypeAssignment",
              "PassengerStopAssignment", "JourneyPattern", "Line", "Route", "Operator"}


def archived_file(landing, dataset, day):
    """Newest complete snapshot on/before the operating day. Never fall forward."""
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
    """Yield complete selected subtrees, removing consumed XML from its parents."""
    with zipfile.ZipFile(archive) as z, z.open(member) as source:
        stack, capture = [], None
        for event, elem in ET.iterparse(source, events=("start", "end")):
            if event == "start":
                stack.append(elem)
                if capture is None and local(elem.tag) in tags:
                    capture = len(stack)
            else:
                if capture == len(stack):
                    yield strip_ns(elem)
                    capture = None
                if capture is None:
                    if len(stack) > 1:
                        stack[-2].remove(elem)
                    elem.clear()
                stack.pop()


def index_member(archive, member, day):
    index = {k: {} for k in INDEX_TAGS}
    index["dated"] = defaultdict(list)
    index["dated_services"] = set()
    for elem in elements(archive, member, INDEX_TAGS | {"DatedServiceJourney", "FlexibleLine"}):
        kind, ident = "Line" if elem.tag == "FlexibleLine" else elem.tag, elem.get("id")
        if kind == "DatedServiceJourney":
            # OperatingDay references are resolved after the complete member was read.
            sj = ref(elem, "ServiceJourneyRef")
            index["dated_services"].add(sj)
            index["dated"][sj].append((ident, ref(elem, "OperatingDayRef"), text(elem, "ServiceAlteration")))
            continue
        # Keep compact primitive values, not a DOM of the national archive.
        data = {"id": ident, "version": elem.get("version"), "name": text(elem, "Name")}
        if kind == "OperatingDay":
            data["date"] = text(elem, "CalendarDate")
        elif kind == "OperatingPeriod":
            data.update(start=text(elem, "FromDate"), end=text(elem, "ToDate"),
                        start_ref=ref(elem, "FromOperatingDayRef"), end_ref=ref(elem, "ToOperatingDayRef"))
        elif kind == "DayType":
            data["weekdays"] = [word for e in elem.findall("properties/PropertyOfDay/DaysOfWeek") for word in (e.text or "").split()]
            data["unsupported"] = any(local(e.tag) not in {"properties", "PropertyOfDay", "DaysOfWeek", "Name", "Description"}
                                      for e in elem.iter() if e is not elem)
        elif kind == "DayTypeAssignment":
            availability = text(elem, "isAvailable")
            invalid = []
            available = True if availability is None else boolean(availability, invalid, "isAvailable")
            if invalid:
                raise ValueError(f"invalid availability in DayTypeAssignment {ident}")
            data.update(day_type=ref(elem, "DayTypeRef"), date=text(elem, "Date"),
                        day_ref=ref(elem, "OperatingDayRef"), period=ref(elem, "OperatingPeriodRef"),
                        available=available)
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
                    alighting=boolean(text(point, "ForAlighting"), issues, "alighting"), issues=issues)
        elif kind == "Line":
            data.update(operator_id=ref(elem, "OperatorRef"), mode=canonical_mode(text(elem, "TransportMode")),
                        public_code=text(elem, "PublicCode"), is_flexible=elem.tag == "FlexibleLine")
        elif kind == "Route":
            data["direction"] = text(elem, "DirectionType")
            data["line_id"] = ref(elem, "LineRef") or ref(elem, "FlexibleLineRef")
        # FIN repeats a child assignment id across different DayTypeRef values.
        # Its date rule is a relation, so retain each distinct selector rather
        # than overwriting a different calendar's exception by native id.
        key = (ident, data["day_type"], data["date"], data["day_ref"], data["period"], data["available"]) if kind == "DayTypeAssignment" else ident
        previous = index[kind].get(key)
        if previous and previous != data:
            raise ValueError(f"conflicting {kind} {ident} in {member}")
        index[kind][key] = data
    return index


@lru_cache(maxsize=2)
def shared_index(archive, member, day):
    return index_member(archive, member, day)


class Calendar:
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
        if not record:
            for fallback in self.fallbacks:
                if ident in fallback.maps["OperatingDay"]:
                    return fallback.operating_day(ident)
            raise ValueError(f"unresolved OperatingDay {ident}")
        return date.fromisoformat(record["date"])

    def active(self, ident):
        if ident in self.cache:
            return self.cache[ident]
        dtype = self.maps["DayType"].get(ident)
        if dtype is None:
            for fallback in self.fallbacks:
                if ident in fallback.maps["DayType"]:
                    # Resolve the whole date rule in that shared file's scope:
                    # generic OperatingPeriod ids can overlap between exports.
                    self.cache[ident] = fallback.active(ident)
                    return self.cache[ident]
        if dtype is None or dtype["unsupported"]:
            raise ValueError(f"missing or unsupported DayType {ident}")
        weekdays = set(dtype["weekdays"])
        today = self.day.strftime("%A")
        weekday_ok = not weekdays or today in weekdays or "Everyday" in weekdays or \
            ("Weekdays" in weekdays and self.day.weekday() < 5) or \
            ("Weekend" in weekdays and self.day.weekday() >= 5)
        known = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
                 "Everyday", "Weekdays", "Weekend", "None"}
        if weekdays - known:
            raise ValueError(f"unsupported DaysOfWeek {weekdays - known}")
        included, excluded = False, False
        for a in self.assignments.get(ident, []):
            exact = a["date"] or (str(self.operating_day(a["day_ref"])) if a["day_ref"] else None)
            if exact:
                applies = date.fromisoformat(exact) == self.day
            elif a["period"]:
                p = self.maps["OperatingPeriod"].get(a["period"])
                if p is None:
                    raise ValueError(f"unresolved OperatingPeriod {a['period']}")
                start = date.fromisoformat(p["start"][:10]) if p["start"] else self.operating_day(p["start_ref"])
                end = date.fromisoformat(p["end"][:10]) if p["end"] else self.operating_day(p["end_ref"])
                applies = start <= self.day <= end and weekday_ok
            else:
                raise ValueError(f"unsupported DayTypeAssignment {a['id']}")
            included |= applies and a["available"]
            excluded |= applies and not a["available"]
        self.cache[ident] = included and not excluded
        return self.cache[ident]


def planned_time(value, offset, day, issues, field):
    """Local timetable clock + day offset -> UTC; flag ambiguous/missing DST times."""
    if value is None:
        return None
    try:
        clock = time.fromisoformat(value)
        dt = datetime.combine(day + timedelta(days=int(offset or 0)), clock)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc)
        candidates = {dt.replace(tzinfo=OSLO, fold=fold).astimezone(timezone.utc)
                      for fold in (0, 1)
                      if dt.replace(tzinfo=OSLO, fold=fold).astimezone(timezone.utc).astimezone(OSLO).replace(tzinfo=None) == dt}
        if len(candidates) != 1:
            issues.append(f"{'ambiguous' if candidates else 'nonexistent'}_{field}")
            return None
        return candidates.pop()
    except (ValueError, TypeError):
        issues.append(f"invalid_{field}")
        return None


def plan_member(archive, member, shared, day):
    own = index_member(archive, member, day)
    # Several SKY/RUT flexible members have ordinary place-name filenames.
    # Their Line element, rather than the filename, chooses the calendar scope.
    if own["Line"]:
        scopes = {line["is_flexible"] for line in own["Line"].values()}
        if len(scopes) > 1:
            raise ValueError(f"mixed fixed/flexible calendar scopes in {member}")
        codespace = member.split("_", 1)[0]
        preferred = f"_{codespace}{'_flexible' if True in scopes else ''}_shared_data.xml"
        with zipfile.ZipFile(archive) as z:
            if preferred in z.namelist():
                shared = preferred
    indexes = [own] + ([shared_index(archive, shared, day)] if shared else [])
    codespace = member.split("_", 1)[0]
    with zipfile.ZipFile(archive) as z:
        alternatives = [n for n in (f"_{codespace}_shared_data.xml", f"_{codespace}_flexible_shared_data.xml")
                        if n != shared and n != member and n in z.namelist()]
    fallbacks = [Calendar([own, shared_index(archive, n, day)], day) for n in alternatives]
    calendar = Calendar(indexes, day, fallbacks)
    maps = calendar.maps
    stop_quays = defaultdict(set)
    for assignment in maps["PassengerStopAssignment"].values():
        if assignment["quay"]:
            stop_quays[assignment["stop"]].add(assignment["quay"])
    for line in own["Line"].values():
        yield dict(kind="line", **line, source_member=member, dq_issue=[])
    for operator in own["Operator"].values():
        yield dict(kind="operator", **operator, source_member=member, dq_issue=[])
    dated = defaultdict(list)
    for index in indexes:
        for sj, versions in index["dated"].items():
            for ident, day_ref, alteration in versions:
                if calendar.operating_day(day_ref) == day:
                    dated[sj].append((ident, alteration))
    dated_services = set().union(*(i["dated_services"] for i in indexes))
    for sj in elements(archive, member, {"ServiceJourney"}):
        jid = sj.get("id")
        types = [e.get("ref") for e in sj.findall("dayTypes/DayTypeRef")]
        if jid in dated_services:
            active = bool(dated[jid])
        elif types:
            active = any(calendar.active(t) for t in types)
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
        base = dict(kind="journey", journey_id=jid, operating_date=day,
                    line_id=line_id, operator_id=ref(sj, "OperatorRef") or line.get("operator_id"),
                    mode=canonical_mode(text(sj, "TransportMode")) or line.get("mode"),
                    direction_type=text(sj, "DirectionType") or pattern["direction"] or route.get("direction"),
                    journey_pattern_id=pattern_id, source_member=member,
                    is_flexible=line.get("is_flexible", False),
                    service_alteration=next(iter(alterations)) if len(alterations) == 1 else None,
                    dq_issue=issues, dated_journey_ids=[ident for ident, _ in aliases])
        yield base
        passing = sj.findall("passingTimes/TimetabledPassingTime")
        if not passing:
            raise ValueError(f"unsupported timetable without passing times: {jid}")
        for point in passing:
            pi = list(issues)
            point_id = ref(point, "StopPointInJourneyPatternRef") or ref(point, "PointInJourneyPatternRef")
            p = pattern["points"].get(point_id)
            if p is None:
                raise ValueError(f"unresolved stop position {point_id} for {jid}")
            pi.extend(p["issues"])
            quays = stop_quays[p["stop"]]
            if len(quays) != 1:
                pi.append("missing_quay_mapping" if not quays else "ambiguous_quay_mapping")
            yield dict(base, kind="call", dq_issue=pi, call_order=p["order"],
                       quay_id=next(iter(quays)) if len(quays) == 1 else None,
                       for_boarding=p["boarding"], for_alighting=p["alighting"],
                       planned_arrival_at=planned_time(text(point, "ArrivalTime"), text(point, "ArrivalDayOffset"), day, pi, "planned_arrival_at"),
                       planned_departure_at=planned_time(text(point, "DepartureTime"), text(point, "DepartureDayOffset"), day, pi, "planned_departure_at"),
                       earliest_departure_at=planned_time(text(point, "EarliestDepartureTime"), text(point, "EarliestDepartureDayOffset"), day, pi, "earliest_departure_at"),
                       latest_arrival_at=planned_time(text(point, "LatestArrivalTime"), text(point, "LatestArrivalDayOffset"), day, pi, "latest_arrival_at"))


def plan_partition(items, archive, day):
    for member, shared in items:
        yield from plan_member(archive, member, shared, day)


def plan_members(archive):
    with zipfile.ZipFile(archive) as z:
        names = set(z.namelist())
    members = []
    for name in sorted(names):
        if name.startswith("_") or not name.endswith(".xml"):
            continue
        codespace = name.split("_", 1)[0]
        flexible = "FlexibleLine" in name
        shared = f"_{codespace}{'_flexible' if flexible else ''}_shared_data.xml"
        if shared not in names:
            alternative = f"_{codespace}{'' if flexible else '_flexible'}_shared_data.xml"
            shared = alternative if alternative in names else None
        members.append((name, shared))
    return members


def nsr_partition(members, archive):
    for member in members:
        for stop in elements(archive, member, {"StopPlace"}):
            def entity(elem, kind, parent=None):
                issues = []
                return dict(kind=kind, id=elem.get("id"), version=elem.get("version"),
                            name=text(elem, "Name"), stop_place_id=parent,
                            parent_stop_place_id=ref(elem, "ParentSiteRef"),
                            latitude=number(text(elem, "Centroid/Location/Latitude"), issues, "latitude"),
                            longitude=number(text(elem, "Centroid/Location/Longitude"), issues, "longitude"),
                            source_member=member, dq_issue=issues)
            yield entity(stop, "stop_place")
            for quay in stop.findall("quays/Quay"):
                yield entity(quay, "quay", stop.get("id"))


def reference_schema():
    from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType, IntegerType,
                                   StringType, StructField, StructType, TimestampType)
    fields = {k: StringType() for k in ("kind", "id", "version", "name", "source_member", "public_code",
                                      "journey_id", "line_id", "operator_id", "direction_type", "mode",
                                      "journey_pattern_id", "service_alteration", "quay_id", "stop_place_id", "parent_stop_place_id")}
    fields.update(operating_date=DateType(), call_order=IntegerType(),
                  planned_arrival_at=TimestampType(), planned_departure_at=TimestampType(),
                  earliest_departure_at=TimestampType(), latest_arrival_at=TimestampType(),
                  for_boarding=BooleanType(), for_alighting=BooleanType(), is_flexible=BooleanType(),
                  latitude=DoubleType(), longitude=DoubleType(),
                  dq_issue=ArrayType(StringType(), False), dated_journey_ids=ArrayType(StringType(), False))
    return StructType([StructField(k, v) for k, v in fields.items()])
