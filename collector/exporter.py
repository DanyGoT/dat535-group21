#!/usr/bin/env python3
"""Read-only Prometheus exporter for the collector.

Serves GET /metrics in the Prometheus text format. Every scrape looks at the landing
zone (file sizes and names only, never contents), the receiver's and the DATEX poller's
state files, the monitor's status.json, disk and memory usage. Never writes anything. Bind to localhost only:
Prometheus scrapes it, nothing else should.

Environment:
    DAT535_LANDING_DIR      landing root (default: ./landing)
    RECEIVER_STATE_FILE     written by the receiver (default: ./run/receiver.json)
    DATEX_STATE_FILE        written by the DATEX poller (default: ./run/datex.json)
    MONITOR_FEEDS           feeds to report (default: see monitor.feeds_from_env)
    MONITOR_STATUS_DIR      where the monitor writes status.json (default: ./status)
    EXPORTER_BIND           default: 127.0.0.1
    EXPORTER_PORT           default: 8752
"""

import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from monitor import DATEX_PREFIX, feeds_from_env, landing_dir, parse_iso, pid_alive

HOUR_FILE = re.compile(r"hour=(\d{2})\.jsonl(\.gz|\.part)?$")
DATE_DIR = re.compile(r"date=(\d{4}-\d{2}-\d{2})$")

HELP = {
    "dat535_landing_closed_bytes": ("gauge", "Bytes on disk in closed hourly files (gzip)."),
    "dat535_landing_closed_files": ("gauge", "Closed hourly files."),
    "dat535_landing_open_bytes": ("gauge", "Size of the newest .part file (uncompressed, restarts every hour)."),
    "dat535_landing_open_modified_timestamp_seconds": ("gauge", "Last write to the newest .part file."),
    "dat535_landing_pending_parts": ("gauge", "Older .part files not yet compressed (normal for minutes after the hour, stuck if it stays)."),
    "dat535_landing_first_hour_timestamp_seconds": ("gauge", "Start of the oldest hour in the landing zone."),
    "dat535_landing_day_bytes": ("gauge", "Bytes on disk in closed hourly files per UTC date."),
    "dat535_landing_day_hours": ("gauge", "Closed hours per UTC date (24 = complete day)."),
    "dat535_receiver_up": ("gauge", "1 if the receiver process is alive."),
    "dat535_receiver_start_timestamp_seconds": ("gauge", "When the receiver process started."),
    "dat535_receiver_subscribing": ("gauge", "1 if the receiver holds Entur subscriptions (public URL set)."),
    "dat535_subscription_last_push_timestamp_seconds": ("gauge", "Last push (data or heartbeat) from Entur."),
    "dat535_subscription_expiry_timestamp_seconds": ("gauge", "When the current subscription terminates at Entur."),
    "dat535_subscription_error": ("gauge", "1 if the last subscribe attempt failed."),
    "dat535_datex_up": ("gauge", "1 if the DATEX poller process is alive."),
    "dat535_datex_polling": ("gauge", "1 if the DATEX poller has credentials and polls (0 = idle)."),
    "dat535_datex_start_timestamp_seconds": ("gauge", "When the DATEX poller process started."),
    "dat535_datex_polls_total": ("counter", "DATEX polls since the poller started, by result (changed, unchanged, error)."),
    "dat535_datex_last_ok_timestamp_seconds": ("gauge", "Last successful DATEX poll."),
    "dat535_datex_last_change_timestamp_seconds": ("gauge", "Last DATEX poll that returned a new snapshot."),
    "dat535_datex_poll_interval_seconds": ("gauge", "Configured DATEX poll interval."),
    "dat535_datex_consecutive_errors": ("gauge", "DATEX polls failed in a row (0 = last poll succeeded)."),
    "dat535_datex_snapshot_bytes": ("gauge", "Size of the last new DATEX snapshot (decoded)."),
    "dat535_monitor_ok": ("gauge", "1 if all monitor checks passed on the last run."),
    "dat535_monitor_check_ok": ("gauge", "1 if this monitor check passed on the last run."),
    "dat535_monitor_last_run_timestamp_seconds": ("gauge", "When the monitor last ran."),
    "dat535_memory_total_bytes": ("gauge", "Physical memory (MemTotal)."),
    "dat535_memory_available_bytes": ("gauge", "Memory available to new processes without swapping (MemAvailable)."),
    "dat535_swap_used_bytes": ("gauge", "Swap in use (SwapTotal - SwapFree)."),
    "dat535_filesystem_size_bytes": ("gauge", "Size of the filesystem holding this volume."),
    "dat535_filesystem_free_bytes": ("gauge", "Free bytes on the filesystem holding this volume."),
    "dat535_exporter_scrape_duration_seconds": ("gauge", "Time spent collecting these metrics."),
}


def hour_ts(date, hour):
    return datetime.strptime(f"{date} {hour}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc).timestamp()


def collect_landing(landing, feed, out):
    root = Path(landing) / landing_dir(feed)
    if not root.is_dir():
        return
    closed_bytes = closed_files = 0
    days = {}
    first = None
    parts = []
    for d in sorted(root.iterdir()):
        m = DATE_DIR.match(d.name)
        if not m or not d.is_dir():
            continue
        date = m.group(1)
        day_bytes, day_hours = 0, set()
        for f in d.iterdir():
            fm = HOUR_FILE.match(f.name)
            if not fm:
                continue  # *.gz.tmp while compressing, anything unexpected
            hour, ext = fm.groups()
            try:
                st = f.stat()
            except FileNotFoundError:
                continue  # rotated away between listing and stat
            first = min(first, (date, hour)) if first else (date, hour)
            if ext == ".part":
                parts.append(((date, hour), st))
                continue
            closed_bytes += st.st_size
            closed_files += 1
            day_bytes += st.st_size
            day_hours.add(hour)
        if day_hours:
            days[date] = (day_bytes, len(day_hours))

    lab = {"feed": feed}
    out.append(("dat535_landing_closed_bytes", lab, closed_bytes))
    out.append(("dat535_landing_closed_files", lab, closed_files))
    # The newest .part is the one being written; older ones wait for (or failed) compression.
    out.append(("dat535_landing_pending_parts", lab, max(len(parts) - 1, 0)))
    if parts:
        _, st = max(parts, key=lambda p: p[0])
        out.append(("dat535_landing_open_bytes", lab, st.st_size))
        out.append(("dat535_landing_open_modified_timestamp_seconds", lab, st.st_mtime))
    if first:
        out.append(("dat535_landing_first_hour_timestamp_seconds", lab, hour_ts(*first)))
    for date, (b, h) in days.items():
        out.append(("dat535_landing_day_bytes", {"feed": feed, "date": date}, b))
        out.append(("dat535_landing_day_hours", {"feed": feed, "date": date}, h))


def collect_receiver(state_file, out):
    try:
        state = json.loads(Path(state_file).read_text())
    except (OSError, ValueError):
        out.append(("dat535_receiver_up", {}, 0))
        return
    out.append(("dat535_receiver_up", {}, int(pid_alive(int(state.get("pid", 0))))))
    started = parse_iso(state.get("started_at"))
    if started:
        out.append(("dat535_receiver_start_timestamp_seconds", {}, started.timestamp()))
    out.append(("dat535_receiver_subscribing", {}, int(bool(state.get("subscribing")))))
    for feed, s in (state.get("feeds") or {}).items():
        lab = {"feed": feed}
        for key, name in (("last_push", "dat535_subscription_last_push_timestamp_seconds"),
                          ("expires_at", "dat535_subscription_expiry_timestamp_seconds")):
            t = parse_iso(s.get(key))
            if t:
                out.append((name, lab, t.timestamp()))
        out.append(("dat535_subscription_error", lab, int(bool(s.get("last_error")))))


def collect_datex(state_file, out):
    try:
        state = json.loads(Path(state_file).read_text())
    except (OSError, ValueError):
        out.append(("dat535_datex_up", {}, 0))
        return
    out.append(("dat535_datex_up", {}, int(pid_alive(int(state.get("pid", 0))))))
    out.append(("dat535_datex_polling", {}, int(bool(state.get("polling")))))
    started = parse_iso(state.get("started_at"))
    if started:
        out.append(("dat535_datex_start_timestamp_seconds", {}, started.timestamp()))
    for feed, s in (state.get("feeds") or {}).items():
        lab = {"feed": f"datex-{feed}"}     # same label value as the landing metrics
        for result, n in (s.get("polls") or {}).items():
            out.append(("dat535_datex_polls_total", {**lab, "result": result}, n))
        for key, name in (("last_ok", "dat535_datex_last_ok_timestamp_seconds"),
                          ("last_change", "dat535_datex_last_change_timestamp_seconds")):
            t = parse_iso(s.get(key))
            if t:
                out.append((name, lab, t.timestamp()))
        out.append(("dat535_datex_poll_interval_seconds", lab, s.get("interval_s") or 0))
        out.append(("dat535_datex_consecutive_errors", lab, s.get("errors") or 0))
        if s.get("last_bytes") is not None:
            out.append(("dat535_datex_snapshot_bytes", lab, s["last_bytes"]))


def collect_monitor(status_dir, out):
    try:
        status = json.loads((Path(status_dir) / "status.json").read_text())
    except (OSError, ValueError):
        return
    out.append(("dat535_monitor_ok", {}, int(bool(status.get("ok")))))
    for check, c in (status.get("checks") or {}).items():
        out.append(("dat535_monitor_check_ok", {"check": check}, int(bool(c.get("ok")))))
    t = parse_iso(status.get("checked_at"))
    if t:
        out.append(("dat535_monitor_last_run_timestamp_seconds", {}, t.timestamp()))


def collect_memory(out, meminfo="/proc/meminfo"):
    kb = {}
    with open(meminfo) as fh:
        for line in fh:
            key, _, rest = line.partition(":")
            kb[key] = int(rest.split()[0])
    out.append(("dat535_memory_total_bytes", {}, kb["MemTotal"] * 1024))
    out.append(("dat535_memory_available_bytes", {}, kb["MemAvailable"] * 1024))
    out.append(("dat535_swap_used_bytes", {}, (kb.get("SwapTotal", 0) - kb.get("SwapFree", 0)) * 1024))


def collect_disk(volumes, out):
    for volume, path in volumes.items():
        try:
            u = shutil.disk_usage(path)
        except OSError:
            continue
        out.append(("dat535_filesystem_size_bytes", {"volume": volume}, u.total))
        out.append(("dat535_filesystem_free_bytes", {"volume": volume}, u.free))


def label_str(labels):
    if not labels:
        return ""
    esc = lambda v: str(v).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
    return "{" + ",".join(f'{k}="{esc(v)}"' for k, v in labels.items()) + "}"


def render(samples):
    by_name = {}
    for name, labels, value in samples:
        by_name.setdefault(name, []).append((labels, value))
    lines = []
    for name, rows in by_name.items():
        kind, text = HELP[name]
        lines.append(f"# HELP {name} {text}")
        lines.append(f"# TYPE {name} {kind}")
        for labels, value in rows:
            lines.append(f"{name}{label_str(labels)} {value:.17g}" if isinstance(value, float)
                         else f"{name}{label_str(labels)} {value}")
    return "\n".join(lines) + "\n"


class Collector:
    def __init__(self, landing, state_file, feeds, status_dir, datex_state_file=None):
        self.landing = landing
        self.state_file = state_file
        self.datex_state_file = datex_state_file
        self.feeds = feeds
        self.status_dir = status_dir
        self.volumes = {"landing": landing, "home": str(Path.home())}

    def collect(self):
        start = time.monotonic()
        out = []
        steps = [lambda f=f: collect_landing(self.landing, f, out) for f in self.feeds]
        if self.datex_state_file and any(f.startswith(DATEX_PREFIX) for f in self.feeds):
            steps.append(lambda: collect_datex(self.datex_state_file, out))
        steps += [lambda: collect_receiver(self.state_file, out),
                  lambda: collect_monitor(self.status_dir, out),
                  lambda: collect_memory(out),
                  lambda: collect_disk(self.volumes, out)]
        for step in steps:
            try:
                step()
            except Exception as e:  # one broken source must not hide the others
                print(f"collect failed: {e!r}", file=sys.stderr, flush=True)
        out.append(("dat535_exporter_scrape_duration_seconds", {}, time.monotonic() - start))
        return render(out)


class Handler(BaseHTTPRequestHandler):
    server_version = "exporter"
    sys_version = ""
    collector: "Collector | None" = None

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path.split("?", 1)[0] != "/metrics":
            self.send_error(404)
            return
        body = self.collector.collect().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    Handler.collector = Collector(os.environ.get("DAT535_LANDING_DIR", "landing"),
                                  os.environ.get("RECEIVER_STATE_FILE", "run/receiver.json"),
                                  feeds_from_env(), os.environ.get("MONITOR_STATUS_DIR", "status"),
                                  os.environ.get("DATEX_STATE_FILE", "run/datex.json"))
    bind = os.environ.get("EXPORTER_BIND", "127.0.0.1")
    port = int(os.environ.get("EXPORTER_PORT", "8752"))
    server = ThreadingHTTPServer((bind, port), Handler)
    print(f"serving metrics on {bind}:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
