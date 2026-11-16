#!/usr/bin/env python3
"""Statens vegvesen (NPRA) DATEX II 3.1 poller.

Pull only: every poll GETs a full snapshot. Each poll, whatever its outcome, becomes one JSON
line in its feed's hourly landing file, <landing>/datex-<feed>/date=D/hour=HH.jsonl.part:

    {"requested_at", "received_at", "endpoint", "http_status", "last_modified", "error", "payload"}

Environment: DAT535_LANDING_DIR, DATEX_USERNAME/DATEX_PASSWORD (unset: idle), DATEX_FEEDS,
DATEX_BASE_URL, DATEX_STATE_FILE, DATEX_SPOOL_DIR, ET_CLIENT_NAME (sent as User-Agent).

    python3 datex.py                      # poll forever (dat535-datex.service)
    python3 datex.py --check [FEED ...]   # one GET per feed, printed; writes nothing
"""

import argparse
import base64
import codecs
import gzip
import hashlib
import http.client
import json
import logging
import os
import re
import signal
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

from hourly import HourlyWriter, iso, utcnow

log = logging.getLogger("datex")


class Feed(NamedTuple):
    service: str  # NPRA service name in the URL
    interval_s: int  # poll interval
    stale_s: "int | None"  # unchanged this long = NPRA stopped publishing (monitor)


# Sizes of one snapshot (2026-10-23, raw / gzipped): situation 28.6 MB / 2.9 MB, the others
# 0.25-1.9 MB / 5-160 KB. ~8.8 GB/day downloaded, ~0.9 GB/day stored, nearly all situations.
# No feed sends Last-Modified or gzip, so every poll downloads the whole snapshot.
FEEDS = {
    # ~1150 road works, closures, accidents. Records carry their own times, so 5 min only
    # misses situations that start and end within 5 min.
    "situation": Feed("GetSituation", 300, None),
    # 404 road sections, updated every 5 min: polled twice as often to skip none.
    "traveltime": Feed("GetTravelTimeData", 150, 1800),
    # 475 road weather stations, updated every 10 min.
    "weather": Feed("GetMeasuredWeatherData", 300, 3600),
    # Where the sections and stations are: changes only when NPRA adds or moves one.
    "traveltime-locations": Feed("GetPredefinedTravelTimeLocations", 6 * 3600, None),
    "weather-sites": Feed("GetMeasurementWeatherSiteTable", 6 * 3600, None),
}
DEFAULT_BASE_URL = "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi"

TIMEOUT_S = 60  # per socket operation, not for the whole download
MAX_BODY = 512 * 1024 * 1024  # decoded; a snapshot this large means something is wrong
BODY_CHUNK = 1 << 20
MAX_BACKOFF_S = 900
AUTH_PAUSE_S = 900  # after 401/403, for every feed: don't lock the account
STAGGER_S = 7  # feeds start this far apart, not all in the same second
SPOOL_PREFIX = "datex-"
ROTATE_CHECK_S = 10


def landing_name(feed):
    return f"datex-{feed}"


def feed_url(base_url, feed):
    return f"{base_url.rstrip('/')}/{FEEDS[feed].service}/pullsnapshotdata"


class Response(NamedTuple):
    status: "int | None"  # None: no HTTP response at all (DNS, connect, timeout)
    last_modified: "str | None"
    error: "str | None"  # None: a 200 with a complete body, or a 304
    size: int = 0  # decoded body bytes passed to the sink
    sha256: "str | None" = None  # of the decoded body, when complete
    encoding: str = ""  # transfer Content-Encoding
    retry_after: "int | None" = None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    # urllib would send our Authorization header to the new host. Returning None makes it
    # raise HTTPError for the 3xx instead.
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


OPENER = urllib.request.build_opener(NoRedirect)


def fetch(url, auth, client_name, if_modified_since, sink, timeout=TIMEOUT_S):
    """GET url; pass a 200's decoded text to sink() piece by piece. Never raises: see Response."""
    headers = {"Authorization": auth, "User-Agent": client_name, "Accept-Encoding": "gzip"}
    # Sent in case NPRA ever starts sending Last-Modified.
    if if_modified_since:
        headers["If-Modified-Since"] = if_modified_since
    status = last_modified = None
    encoding, size = "", 0
    try:
        request = urllib.request.Request(url, headers=headers)
        with OPENER.open(request, timeout=timeout) as resp:
            status = resp.status
            last_modified = resp.headers.get("Last-Modified")
            encoding = resp.headers.get("Content-Encoding", "").strip().lower()

            def failed(error):
                return Response(status, last_modified, error, size, encoding=encoding)

            if status != 200:
                return failed(f"unexpected HTTP {status}")
            if encoding == "gzip":
                body = gzip.GzipFile(fileobj=resp)
            elif encoding in ("", "identity"):
                body = resp
            else:
                return failed(f"unexpected Content-Encoding {encoding!r}")

            # Stream: decode and hash chunk by chunk, never holding the whole body.
            decoder = codecs.getincrementaldecoder("utf-8")()
            digest = hashlib.sha256()
            while chunk := body.read(BODY_CHUNK):
                size += len(chunk)
                if size > MAX_BODY:
                    return failed(f"body larger than {MAX_BODY} bytes")
                digest.update(chunk)
                sink(decoder.decode(chunk))
            # http.client returns a short read instead of raising: check what is missing.
            if resp.length:
                return failed(
                    f"IncompleteRead: connection closed with "
                    f"{resp.length} of Content-Length bytes missing"
                )
            decoder.decode(b"", final=True)  # raises on a truncated UTF-8 sequence at the end
            return Response(status, last_modified, None, size, digest.hexdigest(), encoding)

    except urllib.error.HTTPError as e:
        # The error body is only a hint: a stalled or cut-off one must not lose the status.
        try:
            if e.code == 304:
                detail = ""
            else:
                detail = " ".join(e.read(300).decode("utf-8", "replace").split())
        except (http.client.HTTPException, OSError, ValueError):
            detail = ""
        finally:
            e.close()
        if e.code == 304:
            return Response(304, e.headers.get("Last-Modified"), None)
        if 300 <= e.code < 400 and e.headers.get("Location"):
            detail = f"redirect to {e.headers['Location']} (not followed)"
        retry = e.headers.get("Retry-After", "")
        error = f"HTTP {e.code} {e.reason}" + (f": {detail}" if detail else "")
        return Response(e.code, None, error, retry_after=int(retry) if retry.isdigit() else None)

    except (urllib.error.URLError, http.client.HTTPException, OSError, EOFError, zlib.error,
            ValueError) as e:
        # Timeouts, connections closed mid-body, bad gzip, invalid UTF-8.
        reason = e.reason if isinstance(e, urllib.error.URLError) else e
        error = f"{type(e).__name__}: {reason}"
        return Response(status, last_modified, error, size, encoding=encoding)


class AuthPause:
    """Shared by all feeds: after rejected credentials nobody polls until `until`."""

    def __init__(self):
        self.until = 0.0  # time.monotonic()
        self.reason = None

    def remaining(self):
        return max(0.0, self.until - time.monotonic())


class Poller:
    """Polls one feed in its own thread and keeps the state the monitor reads."""

    def __init__(self, feed, base_url, auth, client_name, writer, spool_dir, pause):
        self.feed, self.spec = feed, FEEDS[feed]
        self.url = feed_url(base_url, feed)
        self.auth, self.client_name = auth, client_name
        self.writer, self.spool_dir, self.pause = writer, spool_dir, pause
        self.stop = threading.Event()  # run() replaces it with the process-wide one
        self.last_modified = None  # sent as If-Modified-Since
        self.last_sha256 = None  # of the last complete 200: tells a new snapshot from a repeat
        self.errors = 0  # consecutive failed polls
        self.lock = threading.Lock()
        self.state = {
            "service": self.spec.service,
            "interval_s": self.spec.interval_s,
            "stale_after_s": self.spec.stale_s,
            "last_poll": None,
            "last_status": None,
            "last_ok": None,
            "last_change": None,
            "last_bytes": None,
            "errors": 0,
            "last_error": None,
            "last_error_at": None,
            "next_poll": None,
            "polls": {"changed": 0, "unchanged": 0, "error": 0},
        }

    def snapshot(self):
        with self.lock:
            return {**self.state, "polls": dict(self.state["polls"])}

    def poll(self):
        """One GET, one landing line. Returns seconds until the next poll."""
        sent = self.last_modified
        requested = utcnow()
        # The body goes to an anonymous spool file first; it is landed only once complete.
        with tempfile.TemporaryFile(dir=self.spool_dir, prefix=SPOOL_PREFIX) as spool:

            def to_spool(text):
                # JSON-escape piece by piece: same bytes as escaping the whole payload at once.
                escaped = json.dumps(text, ensure_ascii=False)[1:-1]
                spool.write(escaped.encode("utf-8"))

            r = fetch(self.url, self.auth, self.client_name, sent, to_spool)
            received = utcnow()
            if self.stop.is_set():
                return 0  # shutting down: don't start a write that might be cut off

            line = {
                "requested_at": iso(requested),
                "received_at": iso(received),
                "endpoint": self.feed,
                "http_status": r.status,
                "last_modified": r.last_modified,
                "error": r.error,
                "payload": None,
            }
            if r.status == 200 and r.error is None:
                # head + spooled payload + tail = json.dumps(line with payload) + "\n".
                head = json.dumps({**line, "payload": ""}, ensure_ascii=False)[:-2]
                spool.seek(0)
                self.writer.append_spooled(head, spool, '"}\n', received)
            else:
                self.writer.append(json.dumps(line, ensure_ascii=False) + "\n", received)
        return self._record(r, received)

    def _record(self, r, now):
        """Update the state after a poll. Returns the delay until the next one."""
        ok = r.error is None and r.status in (200, 304)
        changed = ok and r.status == 200 and r.sha256 != self.last_sha256
        if ok:
            self.errors, delay = 0, self.spec.interval_s
            if r.status == 200:
                self.last_modified, self.last_sha256 = r.last_modified, r.sha256
        else:
            # Exponential backoff, capped at 15 minutes.
            self.errors += 1
            delay = min(self.spec.interval_s * 2 ** (self.errors - 1), MAX_BACKOFF_S)
            # The server's own wait is a minimum, not capped like our backoff.
            if r.retry_after:
                delay = max(delay, r.retry_after)
            if r.status in (401, 403):
                delay = AUTH_PAUSE_S
                self.pause.until = time.monotonic() + delay
                self.pause.reason = f"credentials rejected, all feeds paused: {r.error}"

        with self.lock:
            s = self.state
            s["last_poll"], s["last_status"], s["errors"] = iso(now), r.status, self.errors
            s["next_poll"] = iso(now + timedelta(seconds=delay))
            outcome = "changed" if changed else "unchanged" if ok else "error"
            s["polls"][outcome] += 1
            if ok:
                s["last_ok"] = iso(now)
            else:
                s["last_error"], s["last_error_at"] = r.error, iso(now)
            if changed:
                s["last_change"], s["last_bytes"] = iso(now), r.size

        if not ok:
            log.error(
                "%s: %s (failure %d, next try in %d s)", self.feed, r.error, self.errors, delay
            )
        elif changed:
            log.info("%s: new snapshot, %d bytes", self.feed, r.size)
        return delay

    def run(self, stop, first_delay):
        self.stop = stop
        delay = first_delay
        while not stop.wait(delay):
            paused = self.pause.remaining()
            if paused > 0:
                delay = paused
                continue
            try:
                delay = self.poll()
            except Exception:
                # E.g. landing disk full: keep the feed alive, try again later.
                log.exception("%s: poll failed", self.feed)
                delay = MAX_BACKOFF_S


def write_state(path, pollers, started_at, polling, error):
    state = {
        "pid": os.getpid(),
        "started_at": iso(started_at),
        "updated_at": iso(utcnow()),
        "polling": polling,
        "error": error,
        "feeds": {p.feed: p.snapshot() for p in pollers},
    }
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    os.replace(tmp, path)


def check(base_url, auth, client_name, feeds):
    """One GET per feed; prints what came back, writes nothing. Stops at a rejected login."""
    ok = True
    for feed in feeds:
        url = feed_url(base_url, feed)
        head = []

        def keep_start(text):
            # Enough of the body to find its root element.
            if sum(map(len, head)) < 4096:
                head.append(text)

        started = time.monotonic()
        r = fetch(url, auth, client_name, None, keep_start)
        took = time.monotonic() - started
        root = re.search(r"<(?![?!])([\w.:-]+)", "".join(head))
        print(
            f"{feed}: {url}\n"
            f"  HTTP {r.status} · {r.size:,} bytes"
            f" ({r.encoding or 'uncompressed'} on the wire) · {took:.1f}s"
            f" · Last-Modified {r.last_modified} · root <{root.group(1) if root else '?'}>"
        )
        if r.error:
            print(f"  error: {r.error}")
            ok = False
            if r.status in (401, 403):
                print("  credentials rejected: stopping here, not trying the other feeds")
                break
    return 0 if ok else 1


def main(argv=None):
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--check",
        nargs="*",
        metavar="FEED",
        help="test the credentials and each feed once, write nothing",
    )
    args = p.parse_args(argv)

    # Configuration
    base_url = os.environ.get("DATEX_BASE_URL", DEFAULT_BASE_URL)
    feeds = os.environ.get("DATEX_FEEDS", ",".join(FEEDS)).split(",")
    feeds = [f.strip() for f in feeds if f.strip()]
    if args.check:
        feeds = args.check
    for f in feeds:
        if f not in FEEDS:
            sys.exit(f"unknown feed: {f} (known: {', '.join(FEEDS)})")
    # Credentials go with every request: only over https (or to a local test server).
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme != "https" and parsed.hostname not in ("127.0.0.1", "localhost"):
        sys.exit(f"DATEX_BASE_URL must be https (credentials go with every request): {base_url}")
    user = os.environ.get("DATEX_USERNAME", "")
    password = os.environ.get("DATEX_PASSWORD", "")
    auth = "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    client_name = os.environ.get("ET_CLIENT_NAME", "uis-dat535group21-collector")

    if args.check is not None:
        if not (user and password):
            sys.exit("set DATEX_USERNAME and DATEX_PASSWORD first")
        return check(base_url, auth, client_name, feeds)

    # Folders. The spool is on the landing's filesystem but outside it.
    landing = Path(os.environ.get("DAT535_LANDING_DIR", "landing"))
    state_file = Path(os.environ.get("DATEX_STATE_FILE", "run/datex.json"))
    default_spool = landing.absolute().parent / "datex-spool"
    spool_dir = Path(os.environ.get("DATEX_SPOOL_DIR", default_spool))
    state_file.parent.mkdir(parents=True, exist_ok=True)
    spool_dir.mkdir(parents=True, exist_ok=True)
    # Spool files are anonymous (O_TMPFILE) on Linux; named ones are left only by a crash.
    for leftover in spool_dir.glob(SPOOL_PREFIX + "*"):
        leftover.unlink()

    # One poller thread per feed; this thread rotates files and writes the state.
    started_at = utcnow()
    pause = AuthPause()
    writers = {f: HourlyWriter(landing, landing_name(f)) for f in feeds}
    pollers = [
        Poller(f, base_url, auth, client_name, writers[f], spool_dir, pause) for f in feeds
    ]
    polling = bool(user and password)
    stop = threading.Event()

    def shutdown(signum, _frame):
        log.info("signal %s, shutting down", signum)
        stop.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    threads = []
    if not polling:
        log.warning("DATEX_USERNAME and DATEX_PASSWORD not set: idle, polling nothing")
    else:
        log.info("polling %s at %s into %s", ",".join(feeds), base_url, landing)
        for i, p in enumerate(pollers):
            thread = threading.Thread(
                target=p.run, args=(stop, i * STAGGER_S), daemon=True, name=f"datex-{p.feed}"
            )
            threads.append(thread)
            thread.start()

    while not stop.is_set():
        now = utcnow()
        for w in writers.values():
            w.rotate(now)
        if not polling:
            error = "DATEX_USERNAME and DATEX_PASSWORD not set"
        else:
            error = pause.reason if pause.remaining() > 0 else None
        write_state(state_file, pollers, started_at, polling, error)
        stop.wait(ROTATE_CHECK_S)

    # Idle feeds end at once; a mid-download one stops before writing (see poll()). Give one
    # that is mid-append time to finish its line, then let a download die with the process.
    for t in threads:
        t.join(timeout=10)
    for w in writers.values():
        w.close()
    try:
        state_file.unlink()
    except FileNotFoundError:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
