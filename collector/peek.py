#!/usr/bin/env python3
"""Print a few real messages from the landing zone, parsed and indented. Read-only.

Skips heartbeats; shows one ET journey or SX situation per push, or a few vehicles. Samples
the hour being written at random (topped up from the last closed hours), so every run differs.

    python3 ~/dat535-collector/peek.py et
    python3 ~/dat535-collector/peek.py vehicles -n 3
    python3 ~/dat535-collector/peek.py sx --file <landing>/siri-sx/date=D/hour=12.jsonl.gz

Also used by notebooks/peek.ipynb: sample() for messages, flatten() for pandas rows.
"""

import argparse
import gzip
import json
import os
import random
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from monitor import fmt_bytes, landing_dir

SIRI_NS = "http://www.siri.org.uk/siri"
# Per SIRI feed: the element that carries one unit of data.
SIRI_ITEM = {"et": "EstimatedVehicleJourney", "sx": "PtSituationElement"}
FEEDS = ["et", "sx", "vehicles"]
VEHICLES_SHOWN = 2  # vehicles printed per message (a message holds up to 20)
MAX_TRIES = 200  # random reads before falling back to reading from the start
RECENT_HOURS = 3  # closed hours to top up from when the open hour has too little

ET.register_namespace("", SIRI_NS)


def parse_line(feed, line):
    """One landing line as {received_at, payload_bytes, items}, or None if it holds no data.

    items: the ET journeys / SX situations (XML elements), or the vehicles (dicts).
    """
    try:
        rec = json.loads(line)
        payload = rec["payload"]
        if feed == "vehicles":
            msg = json.loads(payload)
            items = ((msg.get("payload") or {}).get("data") or {}).get("vehicles") or []
            if msg.get("type") != "next":
                items = []
        elif "HeartbeatNotification" in payload[:2000]:
            items = []
        else:
            items = list(ET.fromstring(payload).iter(f"{{{SIRI_NS}}}{SIRI_ITEM[feed]}"))
    except (ValueError, KeyError, ET.ParseError):
        return None  # a line still being written at the end of a .part, or unparseable
    if not items:
        return None
    return {
        "received_at": rec.get("received_at"),
        "payload_bytes": len(payload.encode("utf-8")),
        "items": items,
    }


def to_text(item):
    """An item as readable text: indented XML without the namespace, or indented JSON."""
    if isinstance(item, dict) or isinstance(item, list):
        return json.dumps(item, indent=2, ensure_ascii=False)
    item = ET.fromstring(ET.tostring(item))  # a copy, so indenting leaves the original alone
    ET.indent(item)
    return ET.tostring(item, encoding="unicode").replace(f' xmlns="{SIRI_NS}"', "", 1)


def format_message(feed, msg):
    """(header, body) for printing: one random SIRI item, or a few random vehicles."""
    items = msg["items"]
    if feed == "vehicles":
        shown = random.sample(items, min(VEHICLES_SHOWN, len(items)))
        what = f"{len(shown)} of {len(items)} vehicles in this message"
        body = to_text(shown)
    else:
        what = f"{SIRI_ITEM[feed]} 1 of {len(items)} in this push"
        body = to_text(random.choice(items))
    size = fmt_bytes(msg["payload_bytes"])
    return f"{feed} · received {msg['received_at']} · {size} payload · {what}", body


def local(tag):
    return tag.rsplit("}", 1)[-1]


def flatten(elem):
    """An XML element's leaf values as {path: text}, e.g. "FramedVehicleJourneyRef/...".

    Repeated children (calls) are skipped: flatten each of them separately.
    """
    out = {}

    def walk(e, prefix):
        counts = {}
        for c in e:
            counts[c.tag] = counts.get(c.tag, 0) + 1
        for c in e:
            if counts[c.tag] > 1 and len(c):
                continue
            key = prefix + local(c.tag)
            if len(c):
                walk(c, key + "/")
            elif key not in out:
                out[key] = (c.text or "").strip()

    walk(elem, "")
    return out


def calls(journey):
    """The RecordedCall and EstimatedCall elements of an ET journey as flat dicts, in order."""
    rows = []
    for kind in ("RecordedCall", "EstimatedCall"):
        for c in journey.iter(f"{{{SIRI_NS}}}{kind}"):
            rows.append({"kind": kind, **flatten(c)})
    return rows


def current_part(landing, feed):
    parts = sorted((Path(landing) / landing_dir(feed)).glob("date=*/hour=*.jsonl.part"))
    return parts[-1] if parts else None


def _read_part(path, n, pick):
    """Up to n picked lines of an uncompressed file, from random offsets."""
    size = path.stat().st_size
    found, seen = [], set()
    with open(path, "rb") as fh:
        for _ in range(MAX_TRIES):
            if len(found) == n or size == 0:
                break
            fh.seek(random.randrange(size))
            fh.readline()  # rest of the line we landed in
            start = fh.tell()
            line = fh.readline()
            if start not in seen and line.endswith(b"\n"):
                seen.add(start)
                out = pick(line.decode("utf-8"))
                if out:
                    found.append(out)
        # Data is rare (SX is mostly heartbeats): read the rest from the start.
        fh.seek(0)
        while len(found) < n:
            start = fh.tell()
            line = fh.readline()
            if not line.endswith(b"\n"):
                break
            if start not in seen:
                out = pick(line.decode("utf-8"))
                if out:
                    found.append(out)
    return found


def _read_gz(path, n, pick):
    # A .gz can only be read from the start.
    found = []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            out = pick(line)
            if out:
                found.append(out)
                if len(found) == n:
                    break
    return found


def sample(feed, n=2, path=None, landing=None):
    """Up to n parsed data messages from `path`, else from the hour being written and the last
    closed hours."""

    def pick(line):
        return parse_line(feed, line)

    if path is not None:
        path = Path(path)
        read = _read_gz if path.name.endswith(".gz") else _read_part
        return read(path, n, pick)
    landing = landing or os.environ.get("DAT535_LANDING_DIR", "landing")
    part = current_part(landing, feed)
    found = _read_part(part, n, pick) if part else []
    folder = Path(landing) / landing_dir(feed)
    closed = sorted(folder.glob("date=*/hour=*.jsonl.gz"), reverse=True)
    for gz in closed[:RECENT_HOURS]:
        if len(found) >= n:
            break
        found += _read_gz(gz, n - len(found), pick)
    return found


def main():
    ap = argparse.ArgumentParser(description="Print a few real messages from the landing zone.")
    ap.add_argument("feed", choices=FEEDS)
    ap.add_argument("-n", type=int, default=2, help="messages to show (default: 2)")
    ap.add_argument("--file", type=Path, help="landing file to read (default: the newest .part)")
    args = ap.parse_args()

    found = sample(args.feed, args.n, args.file)
    print(f"# {args.file or 'hour being written, then the last closed hours'}\n")
    for msg in found:
        header, body = format_message(args.feed, msg)
        print(f"── {header}\n{body}\n")
    if len(found) < args.n:
        print(f"# only {len(found)} data message(s) found", file=sys.stderr)


if __name__ == "__main__":
    main()
