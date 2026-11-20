"""Why ingest peaks in rush hour: one UTC day's volume split into its causes, as JSON on stdout.

Run on the VM after Bronze and Silver for the day and its neighbours are complete, under the
Spark lock (it reads Silver) and at low priority (it reads every ET payload once):

    flock -w 10800 /data/dat535/.spark.lock nice -n 19 ionice -c3 \
        ~/spark-env/bin/python rush_hour.py /data/dat535/prod 2026-11-12 > rush-hour-2026-11-12.json

Every series has 24 values, one per UTC landing hour. Reads Bronze, Silver and Prometheus only.
"""

import json
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

HOURS = range(24)
JOURNEY = b"<EstimatedVehicleJourney>"
PROMETHEUS = "http://127.0.0.1:8754/api/v1/query_range"


def zeros():
    return [0] * 24


def files(directory):
    return sorted(Path(directory).glob("*.parquet"))


def field(xml, tag):
    """Text of the first <tag> in an XML fragment, or None."""
    start = xml.find(b"<" + tag + b">")
    if start < 0:
        return None
    start += len(tag) + 2
    return xml[start:xml.find(b"<", start)]


def bronze_volume(root, day):
    """Messages and payload bytes per hour for every Bronze feed but ET (the scan counts ET)."""
    out = {}
    for feed_dir in sorted((root / "bronze").iterdir()):
        if feed_dir.name.startswith("_") or feed_dir.name == "siri-et":
            continue
        messages, size = zeros(), zeros()
        for f in files(feed_dir / f"date={day}"):
            t = pq.read_table(f, columns=["hour", "_bronze_payload_bytes"])
            grouped = t.group_by("hour").aggregate([("_bronze_payload_bytes", "sum"), ([], "count_all")])
            for row in grouped.to_pylist():
                h = int(row["hour"])
                messages[h] += row["count_all"]
                size[h] += row["_bronze_payload_bytes_sum"] or 0
        out[feed_dir.name] = {"messages": messages, "bytes": size}
    return out


def planned_hours(root, day):
    """Planned journeys of operating days D-1..D+1 and the UTC hours of D their trip overlaps.

    Returns (hours by (operating date, ET journey id), active journeys per hour by codespace).
    Journey ids recur daily, so the date keeps a replay of another day's journey out of today's
    hours. Journeys without DataFrameRef (VYG, BNR) carry dated ids and match under date b"" on
    any of the three days. A trip runs from its first planned departure or arrival to its last. Its codespace is
    the one ET reported for it, else its id's prefix.
    """
    day_start = datetime.combine(day, datetime.min.time())
    hours_by_key = defaultdict(set)
    active = defaultdict(zeros)
    for offset in (-1, 0, 1):
        operating = day + timedelta(days=offset)
        t = pq.read_table(
            root / "silver" / "stop_call" / f"operating_date={operating}",
            columns=["journey_id", "source_journey_id", "codespace", "has_plan",
                     "planned_departure_at", "planned_arrival_at"],
            filters=[("has_plan", "=", True)],
        )
        start = pc.min_element_wise(t["planned_departure_at"], t["planned_arrival_at"])
        end = pc.max_element_wise(t["planned_departure_at"], t["planned_arrival_at"])
        trips = pa.table({"journey_id": t["journey_id"], "start": start, "end": end,
                          "codespace": pc.fill_null(t["codespace"], "")})
        trips = trips.group_by("journey_id").aggregate(
            [("start", "min"), ("end", "max"), ("codespace", "max")])
        aliases = pa.table({"journey_id": t["journey_id"], "alias": t["source_journey_id"]})
        aliases = aliases.group_by(["journey_id", "alias"]).aggregate([])

        for trip in trips.to_pylist():
            if trip["start_min"] is None:
                continue
            first = (trip["start_min"] - day_start) // timedelta(hours=1)
            last = (trip["end_max"] - day_start) // timedelta(hours=1)
            hours = set(range(max(first, 0), min(last, 23) + 1))
            if not hours:
                continue
            jid = trip["journey_id"]
            codespace = trip["codespace_max"] or jid.split(":")[0]
            hours_by_key[(str(operating).encode(), jid.encode())] = hours
            hours_by_key[(b"", jid.encode())] |= hours
            for h in hours:
                active[codespace][h] += 1
        for pair in aliases.to_pylist():
            key = (str(operating).encode(), pair["journey_id"].encode())
            if pair["alias"] and key in hours_by_key:
                hours_by_key[(key[0], pair["alias"].encode())] = hours_by_key[key]
                hours_by_key[(b"", pair["alias"].encode())] |= hours_by_key[key]
    return hours_by_key, active


def et_scan(root, day, plan):
    """Every EstimatedVehicleJourney in every ET push of the day, counted by codespace and hour.

    A journey's bytes run from its start tag to the next journey's, so the last one in a message
    also carries the envelope's closing tags (a few hundred bytes).
    """
    totals = {"messages": zeros(), "bytes": zeros(), "no_journey_messages": zeros()}
    keys = ["occurrences", "calls", "bytes", "in_service", "outside_service", "unplanned"]
    by_cs = defaultdict(lambda: {k: zeros() for k in keys})
    seen = defaultdict(lambda: [set() for _ in HOURS])
    for f in files(root / "bronze" / "siri-et" / f"date={day}"):
        print(f"et {f.name}", file=sys.stderr, flush=True)
        reader = pq.ParquetFile(f, buffer_size=8 << 20, pre_buffer=False)
        for batch in reader.iter_batches(batch_size=500, columns=["hour", "payload"]):
            hours = batch.column("hour").to_pylist()
            payloads = batch.column("payload").cast(pa.large_binary()).to_pylist()
            for hour, payload in zip(hours, payloads):
                if payload is None:
                    continue
                h = int(hour)
                totals["messages"][h] += 1
                totals["bytes"][h] += len(payload)
                parts = payload.split(JOURNEY)
                if len(parts) == 1:
                    totals["no_journey_messages"][h] += 1
                for journey in parts[1:]:
                    jid = field(journey, b"DatedVehicleJourneyRef") or field(
                        journey, b"EstimatedVehicleJourneyCode") or b""
                    codespace = (field(journey, b"DataSource") or jid.split(b":")[0]).decode()
                    s = by_cs[codespace]
                    s["occurrences"][h] += 1
                    s["calls"][h] += journey.count(b"<EstimatedCall>") + journey.count(b"<RecordedCall>")
                    s["bytes"][h] += len(journey) + len(JOURNEY)
                    planned = plan.get((field(journey, b"DataFrameRef") or b"", jid))
                    s["unplanned" if planned is None else "in_service" if h in planned
                      else "outside_service"][h] += 1
                    seen[codespace][h].add(jid)
    for codespace, sets in seen.items():
        by_cs[codespace]["reported_journeys"] = [len(s) for s in sets]
    return totals, by_cs


def vehicles(root, day):
    """Vehicle reports and distinct vehicles per hour by codespace, reconnect snapshots included."""
    reports = defaultdict(zeros)
    seen = defaultdict(lambda: [set() for _ in HOURS])
    for f in files(root / "silver" / "_decoded" / "vehicles" / f"source_date={day}"):
        t = pq.read_table(f, columns=["kind", "received_at", "codespace", "vehicle_id"],
                          filters=[("kind", "=", "vehicle")])
        t = pa.table({"hour": pc.hour(t["received_at"]), "codespace": pc.fill_null(t["codespace"], ""),
                      "vehicle_id": t["vehicle_id"]})
        for row in t.group_by(["hour", "codespace", "vehicle_id"]).aggregate([([], "count_all")]).to_pylist():
            reports[row["codespace"]][row["hour"]] += row["count_all"]
            seen[row["codespace"]][row["hour"]].add(row["vehicle_id"])
    return {cs: {"reports": reports[cs], "active": [len(s) for s in seen[cs]]} for cs in reports}


def panel(day):
    """The Grafana ingest panel's input: largest uncompressed .part size per feed and hour."""
    start = datetime.combine(day, datetime.min.time(), timezone.utc) + timedelta(minutes=59, seconds=30)
    query = urllib.parse.urlencode({
        "query": "max by (feed) (max_over_time(dat535_landing_open_bytes[59m]))",
        "start": start.timestamp(), "end": (start + timedelta(hours=23)).timestamp(), "step": 3600,
    })
    with urllib.request.urlopen(f"{PROMETHEUS}?{query}", timeout=30) as response:
        result = json.load(response)["data"]["result"]
    out = {}
    for series in result:
        values = zeros()
        for ts, value in series["values"]:
            values[datetime.fromtimestamp(ts, timezone.utc).hour] = int(float(value))
        out[series["metric"]["feed"]] = values
    return out


def main():
    root, day = Path(sys.argv[1]), date.fromisoformat(sys.argv[2])
    plan, active = planned_hours(root, day)
    totals, by_cs = et_scan(root, day, plan)
    for codespace, counts in active.items():
        by_cs[codespace]["planned_active"] = counts
    json.dump({
        "date": str(day),
        "hours": "UTC; Oslo is UTC+1 in November",
        "panel_uncompressed_bytes": panel(day),
        "bronze": bronze_volume(root, day),
        "et": dict(totals, by_codespace=by_cs),
        "vehicles": vehicles(root, day),
    }, sys.stdout, indent=1)


if __name__ == "__main__":
    main()
