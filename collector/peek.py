#!/usr/bin/env python3
"""Print a few real messages from the landing zone, formatted for reading. Read-only.

Skips heartbeats and other protocol messages and shows the data itself:

    et        one EstimatedVehicleJourney per push, as indented XML
    sx        one PtSituationElement per push, as indented XML
    vehicles  a few vehicles per message, as indented JSON

By default it samples the hour being written (newest .part) at random offsets, so every run
shows different messages. A closed hour (.jsonl.gz) can only be read from the start, so
--file on one shows its first data messages instead.

    python3 ~/dat535-collector/peek.py et
    python3 ~/dat535-collector/peek.py vehicles -n 3
    python3 ~/dat535-collector/peek.py sx --file /data/dat535/landing/siri-sx/date=2026-10-17/hour=12.jsonl.gz

Environment:
    DAT535_LANDING_DIR      landing root (default: ./landing)
"""

import argparse
import gzip
import json
import os
import random
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from monitor import current_part, fmt_bytes

SIRI_NS = "http://www.siri.org.uk/siri"
# Per SIRI feed: the element that carries one unit of data.
SIRI_ITEM = {"et": "EstimatedVehicleJourney", "sx": "PtSituationElement"}
VEHICLES_PER_MESSAGE = 2
MAX_TRIES = 200             # random reads before giving up (a feed of nothing but heartbeats)

ET.register_namespace("", SIRI_NS)


def format_siri(feed, payload):
    """One data element of a SIRI push as indented XML plus a count, or None for a heartbeat."""
    if "HeartbeatNotification" in payload[:2000]:
        return None
    items = list(ET.fromstring(payload).iter(f"{{{SIRI_NS}}}{SIRI_ITEM[feed]}"))
    if not items:
        return None
    item = random.choice(items)
    ET.indent(item)
    text = ET.tostring(item, encoding="unicode").replace(f' xmlns="{SIRI_NS}"', "", 1)
    return f"{SIRI_ITEM[feed]} 1 of {len(items)} in this push", text


def format_vehicles(payload):
    """A few vehicles of a subscription message as indented JSON, or None for anything else."""
    msg = json.loads(payload)
    vehicles = ((msg.get("payload") or {}).get("data") or {}).get("vehicles") or []
    if msg.get("type") != "next" or not vehicles:
        return None
    shown = random.sample(vehicles, min(VEHICLES_PER_MESSAGE, len(vehicles)))
    return (f"{len(shown)} of {len(vehicles)} vehicles in this message",
            json.dumps(shown, indent=2, ensure_ascii=False))


def format_line(feed, line):
    """(header, body) for one landing line, or None if it is not data."""
    try:
        rec = json.loads(line)
        payload = rec["payload"]
        formatted = format_vehicles(payload) if feed == "vehicles" else format_siri(feed, payload)
    except (ValueError, KeyError, ET.ParseError):
        return None     # a line still being written at the end of a .part, or unparseable
    if formatted is None:
        return None
    what, body = formatted
    return f"{feed} · received {rec.get('received_at')} · {fmt_bytes(len(payload))} payload · {what}", body


def sample_part(path, feed, n):
    """Random lines of an uncompressed file: seek, skip to the next full line, keep data.
    Falls back to reading from the start when data is rare (SX is mostly heartbeats)."""
    size = path.stat().st_size
    found, seen = [], set()
    with open(path, "rb") as fh:
        for _ in range(MAX_TRIES):
            if len(found) == n or size == 0:
                break
            fh.seek(random.randrange(size))
            fh.readline()                       # rest of the line we landed in
            start = fh.tell()
            line = fh.readline()
            if start not in seen and line.endswith(b"\n"):
                seen.add(start)
                out = format_line(feed, line.decode("utf-8"))
                if out:
                    found.append(out)
        fh.seek(0)
        while len(found) < n:
            start = fh.tell()
            line = fh.readline()
            if not line.endswith(b"\n"):
                break
            if start not in seen:
                out = format_line(feed, line.decode("utf-8"))
                if out:
                    found.append(out)
    return found


def first_in_gz(path, feed, n):
    found = []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            out = format_line(feed, line)
            if out:
                found.append(out)
                if len(found) == n:
                    break
    return found


def main():
    ap = argparse.ArgumentParser(description="Print a few real messages from the landing zone.")
    ap.add_argument("feed", choices=["et", "sx", "vehicles"])
    ap.add_argument("-n", type=int, default=2, help="messages to show (default: 2)")
    ap.add_argument("--file", type=Path, help="landing file to read (default: the newest .part)")
    args = ap.parse_args()

    path = args.file or current_part(os.environ.get("DAT535_LANDING_DIR", "landing"), args.feed)
    if path is None:
        sys.exit(f"no open hour for {args.feed}; pass --file")
    found = (first_in_gz if path.name.endswith(".gz") else sample_part)(path, args.feed, args.n)
    print(f"# {path}\n")
    for header, body in found:
        print(f"── {header}\n{body}\n")
    if len(found) < args.n:
        print(f"# only {len(found)} data message(s) found", file=sys.stderr)


if __name__ == "__main__":
    main()
