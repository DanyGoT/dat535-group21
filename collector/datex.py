#!/usr/bin/env python3
"""Statens vegvesen (NPRA) DATEX II 3.1 poller.

NPRA's DATEX is pull only: each publication is a snapshot of everything currently valid,
fetched with an HTTP GET (Basic Auth). Every poll, whatever its outcome, is appended as one
JSON line to the hourly landing file of its feed (hourly.py):

    <landing>/datex-<feed>/date=YYYY-MM-DD/hour=HH.jsonl.part  (gzipped to .jsonl.gz when the hour ends)

    {"requested_at", "received_at", "endpoint": "<feed>", "http_status": 200 | 304 | 4xx/5xx | null,
     "last_modified": "<Last-Modified header>" | null, "error": "<what went wrong>" | null,
     "payload": "<XML of a 200, unchanged>" | null}

So the landing zone is the full record of what we asked and what came back: a pull source has
no heartbeats, and the error lines tell a source outage from a collector outage. Bodies are
streamed to an anonymous spool file and appended only once complete and valid UTF-8.

Measured 2026-10-23: no feed sends Last-Modified or gzip, so every poll downloads the whole
snapshot, and an unchanged snapshot is byte-identical to the previous one. Intervals (FEEDS)
are set from those sizes and NPRA's update cadence. Change is detected by hashing the body,
so the monitor can tell when NPRA stops publishing. If-Modified-Since is still sent should a
Last-Modified ever appear, and a 304 is handled.

Politeness: User-Agent names us, feeds start staggered, failures back off up to 15 minutes,
rejected credentials pause every feed for 15 minutes. Redirects are not followed (urllib
would send the Authorization header to the new host) and credentials only travel over https.

Environment:
    DAT535_LANDING_DIR      landing root (default: ./landing)
    DATEX_USERNAME          NPRA DATEX account. Unset: the poller runs idle, polls nothing and
    DATEX_PASSWORD          says so in its state file
    DATEX_FEEDS             comma-separated feeds (default: all of FEEDS)
    DATEX_BASE_URL          default: https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi
    DATEX_STATE_FILE        small JSON status file for the monitor (default: ./run/datex.json)
    DATEX_SPOOL_DIR         responses being received; same filesystem as the landing root,
                            outside it (default: <landing>/../datex-spool)
    ET_CLIENT_NAME          sent as User-Agent (default: uis-dat535group21-collector)

Usage:
    python3 datex.py                      # poll forever (dat535-datex.service)
    python3 datex.py --check [FEED ...]   one GET per feed, printed; writes nothing.
                                          Do the credentials work?
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
    service: str            # NPRA service name in the URL
    interval_s: int         # poll interval
    stale_s: "int | None"   # unchanged for this long = NPRA stopped publishing (monitor)


# One snapshot each, 2026-10-23 (raw / gzipped on disk): situation 28.6 MB / 2.9 MB,
# traveltime 0.25 MB / 5 KB, weather 1.5 MB / 36 KB, traveltime-locations 0.6 MB / 0.16 MB,
# weather-sites 1.9 MB / 44 KB. About 8.8 GB/day downloaded and 0.9 GB/day stored, nearly
# all of it situations.
FEEDS = {
    # 1148 situations (road works, closures, accidents); some record changes every minute.
    # Every 5 min: records carry their own creation/version/validity times, so this only
    # misses situations that start and end within 5 min.
    "situation": Feed("GetSituation", 300, None),
    # 404 road sections, updated every 5 min: polled twice as often so no update is skipped.
    # NPRA marks travel times "end of life".
    "traveltime": Feed("GetTravelTimeData", 150, 1800),
    # 475 road weather stations, 3765 measurements, updated every 10 min.
    "weather": Feed("GetMeasuredWeatherData", 300, 3600),
    # Where the road sections and stations are. Change only when NPRA adds or moves them.
    "traveltime-locations": Feed("GetPredefinedTravelTimeLocations", 6 * 3600, None),
    "weather-sites": Feed("GetMeasurementWeatherSiteTable", 6 * 3600, None),
}
DEFAULT_BASE_URL = "https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi"

TIMEOUT_S = 60                      # per socket operation, not for the whole download
MAX_BODY = 512 * 1024 * 1024        # decoded; a snapshot this large means something is wrong
BODY_CHUNK = 1 << 20
MAX_BACKOFF_S = 900
AUTH_PAUSE_S = 900                  # after 401/403, for every feed: don't lock the account
STAGGER_S = 7                       # feeds start this far apart, not all in the same second
SPOOL_PREFIX = "datex-"
ROTATE_CHECK_S = 10


def landing_name(feed):
    return f"datex-{feed}"


def feed_url(base_url, feed):
    return f"{base_url.rstrip('/')}/{FEEDS[feed].service}/pullsnapshotdata"


class Response(NamedTuple):
    status: "int | None"            # None: no HTTP response at all (DNS, connect, timeout)
    last_modified: "str | None"
    error: "str | None"             # None: a 200 with a complete body, or a 304
    size: int = 0                   # decoded body bytes passed to the sink
    sha256: "str | None" = None     # of the decoded body, when complete
    encoding: str = ""              # transfer Content-Encoding
    retry_after: "int | None" = None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None                 # urllib then raises HTTPError for the 3xx


OPENER = urllib.request.build_opener(NoRedirect)


def fetch(url, auth, client_name, if_modified_since, sink, timeout=TIMEOUT_S):
    """GET url and pass the decoded text of a 200 to sink() piece by piece. Never raises for
    network or HTTP trouble: the Response says what happened."""
    headers = {"Authorization": auth, "User-Agent": client_name, "Accept-Encoding": "gzip"}
    if if_modified_since:
        headers["If-Modified-Since"] = if_modified_since
    status = last_modified = None
    encoding, size = "", 0
    try:
        with OPENER.open(urllib.request.Request(url, headers=headers), timeout=timeout) as resp:
            status = resp.status
            last_modified = resp.headers.get("Last-Modified")
            encoding = resp.headers.get("Content-Encoding", "").strip().lower()
            if status != 200:
                return Response(status, last_modified, f"unexpected HTTP {status}", encoding=encoding)
            if encoding == "gzip":
                body = gzip.GzipFile(fileobj=resp)
            elif encoding in ("", "identity"):
                body = resp
            else:
                return Response(status, last_modified, f"unexpected Content-Encoding {encoding!r}",
                                encoding=encoding)
            decoder = codecs.getincrementaldecoder("utf-8")()
            digest = hashlib.sha256()
            while chunk := body.read(BODY_CHUNK):
                size += len(chunk)
                if size > MAX_BODY:
                    return Response(status, last_modified, f"body larger than {MAX_BODY} bytes",
                                    size, encoding=encoding)
                digest.update(chunk)
                sink(decoder.decode(chunk))
            if resp.length:     # http.client's read(n) returns short instead of raising
                return Response(status, last_modified, f"IncompleteRead: connection closed with "
                                f"{resp.length} of Content-Length bytes missing", size, encoding=encoding)
            decoder.decode(b"", final=True)     # truncated UTF-8 sequence at the end
            return Response(status, last_modified, None, size, digest.hexdigest(), encoding)
    except urllib.error.HTTPError as e:
        try:    # the body is only a hint: a stalled or cut-off one must not lose the status
            detail = "" if e.code == 304 else " ".join(e.read(300).decode("utf-8", "replace").split())
        except (http.client.HTTPException, OSError, ValueError):
            detail = ""
        finally:
            e.close()
        if e.code == 304:
            return Response(304, e.headers.get("Last-Modified"), None)
        if 300 <= e.code < 400 and e.headers.get("Location"):
            detail = f"redirect to {e.headers['Location']} (not followed)"
        retry = e.headers.get("Retry-After", "")
        return Response(e.code, None, f"HTTP {e.code} {e.reason}" + (f": {detail}" if detail else ""),
                        retry_after=int(retry) if retry.isdigit() else None)
    except (urllib.error.URLError, http.client.HTTPException, OSError, EOFError, zlib.error,
            ValueError) as e:
        # Timeouts, connections closed mid-body, bad gzip, invalid UTF-8.
        reason = e.reason if isinstance(e, urllib.error.URLError) else e
        return Response(status, last_modified, f"{type(e).__name__}: {reason}", size, encoding=encoding)


class AuthPause:
    """Shared by all feeds: after rejected credentials nobody polls until `until`."""

    def __init__(self):
        self.until = 0.0            # time.monotonic()
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
        self.stop = threading.Event()   # run() replaces it with the process-wide one
        self.last_modified = None       # sent as If-Modified-Since
        self.last_sha256 = None         # of the last complete 200
        self.errors = 0                 # consecutive failed polls
        self.lock = threading.Lock()
        self.state = {"service": self.spec.service, "interval_s": self.spec.interval_s,
                      "stale_after_s": self.spec.stale_s, "last_poll": None, "last_status": None,
                      "last_ok": None, "last_change": None, "last_bytes": None, "errors": 0,
                      "last_error": None, "last_error_at": None, "next_poll": None,
                      "polls": {"changed": 0, "unchanged": 0, "error": 0}}

    def snapshot(self):
        with self.lock:
            return {**self.state, "polls": dict(self.state["polls"])}

    def poll(self):
        """One GET, one landing line. Returns seconds until the next poll."""
        sent = self.last_modified
        requested = utcnow()
        with tempfile.TemporaryFile(dir=self.spool_dir, prefix=SPOOL_PREFIX) as spool:
            # json.dumps escapes per character, so escaping piece by piece gives the same bytes
            # as escaping the whole payload; the decoder in fetch() holds back split sequences.
            r = fetch(self.url, self.auth, self.client_name, sent,
                      lambda text: spool.write(json.dumps(text, ensure_ascii=False)[1:-1].encode("utf-8")))
            received = utcnow()
            if self.stop.is_set():
                return 0    # shutting down: don't start a write that might be cut off
            line = {"requested_at": iso(requested), "received_at": iso(received),
                    "endpoint": self.feed, "http_status": r.status, "last_modified": r.last_modified,
                    "error": r.error, "payload": None}
            if r.status == 200 and r.error is None:
                # Same bytes as json.dumps({..., "payload": payload}, ensure_ascii=False) + "\n".
                head = json.dumps({**line, "payload": ""}, ensure_ascii=False)[:-2]
                spool.seek(0)
                self.writer.append_spooled(head, spool, '"}\n', received)
            else:
                self.writer.append(json.dumps(line, ensure_ascii=False) + "\n", received)
        return self._record(r, received)

    def _record(self, r, now):
        ok = r.error is None and r.status in (200, 304)
        changed = ok and r.status == 200 and r.sha256 != self.last_sha256
        if ok:
            self.errors, delay = 0, self.spec.interval_s
            if r.status == 200:
                self.last_modified, self.last_sha256 = r.last_modified, r.sha256
        else:
            self.errors += 1
            delay = min(self.spec.interval_s * 2 ** (self.errors - 1), MAX_BACKOFF_S)
            if r.retry_after:   # the server's own wait is a minimum, not capped like our backoff
                delay = max(delay, r.retry_after)
            if r.status in (401, 403):
                delay = AUTH_PAUSE_S
                self.pause.until = time.monotonic() + delay
                self.pause.reason = f"credentials rejected, all feeds paused: {r.error}"

        with self.lock:
            s = self.state
            s["last_poll"], s["last_status"], s["errors"] = iso(now), r.status, self.errors
            s["next_poll"] = iso(now + timedelta(seconds=delay))
            s["polls"]["changed" if changed else "unchanged" if ok else "error"] += 1
            if ok:
                s["last_ok"] = iso(now)
            else:
                s["last_error"], s["last_error_at"] = r.error, iso(now)
            if changed:
                s["last_change"], s["last_bytes"] = iso(now), r.size

        if not ok:
            log.error("%s: %s (failure %d, next try in %d s)", self.feed, r.error, self.errors, delay)
        elif changed:
            log.info("%s: new snapshot, %d bytes", self.feed, r.size)
        return delay

    def run(self, stop, first_delay):
        self.stop = stop
        delay = first_delay
        while not stop.wait(delay):
            if (paused := self.pause.remaining()) > 0:
                delay = paused
                continue
            try:
                delay = self.poll()
            except Exception:   # e.g. landing disk full; keep the feed alive, try again later
                log.exception("%s: poll failed", self.feed)
                delay = MAX_BACKOFF_S


def write_state(path, pollers, started_at, polling, error):
    state = {"pid": os.getpid(), "started_at": iso(started_at), "updated_at": iso(utcnow()),
             "polling": polling, "error": error, "feeds": {p.feed: p.snapshot() for p in pollers}}
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    os.replace(tmp, path)


def check(base_url, auth, client_name, feeds):
    """One GET per feed; prints what came back, writes nothing. Stops at a rejected login."""
    ok = True
    for feed in feeds:
        url = feed_url(base_url, feed)
        head = []
        started = time.monotonic()
        r = fetch(url, auth, client_name, None,
                  lambda text: head.append(text) if sum(map(len, head)) < 4096 else None)
        took = time.monotonic() - started
        root = re.search(r"<(?![?!])([\w.:-]+)", "".join(head))
        print(f"{feed}: {url}\n  HTTP {r.status} · {r.size:,} bytes"
              f" ({r.encoding or 'uncompressed'} on the wire) · {took:.1f}s"
              f" · Last-Modified {r.last_modified} · root <{root.group(1) if root else '?'}>")
        if r.error:
            print(f"  error: {r.error}")
            ok = False
            if r.status in (401, 403):
                print("  credentials rejected: stopping here, not trying the other feeds")
                break
    return 0 if ok else 1


def main(argv=None):
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--check", nargs="*", metavar="FEED",
                   help="test the credentials and each feed once, write nothing")
    args = p.parse_args(argv)

    base_url = os.environ.get("DATEX_BASE_URL", DEFAULT_BASE_URL)
    feeds = [f.strip() for f in os.environ.get("DATEX_FEEDS", ",".join(FEEDS)).split(",") if f.strip()]
    if args.check:
        feeds = args.check
    for f in feeds:
        if f not in FEEDS:
            sys.exit(f"unknown feed: {f} (known: {', '.join(FEEDS)})")
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme != "https" and parsed.hostname not in ("127.0.0.1", "localhost"):
        sys.exit(f"DATEX_BASE_URL must be https (credentials are sent with every request): {base_url}")
    user, password = os.environ.get("DATEX_USERNAME", ""), os.environ.get("DATEX_PASSWORD", "")
    auth = "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    client_name = os.environ.get("ET_CLIENT_NAME", "uis-dat535group21-collector")

    if args.check is not None:
        if not (user and password):
            sys.exit("set DATEX_USERNAME and DATEX_PASSWORD first")
        return check(base_url, auth, client_name, feeds)

    landing = Path(os.environ.get("DAT535_LANDING_DIR", "landing"))
    state_file = Path(os.environ.get("DATEX_STATE_FILE", "run/datex.json"))
    spool_dir = Path(os.environ.get("DATEX_SPOOL_DIR", landing.absolute().parent / "datex-spool"))
    state_file.parent.mkdir(parents=True, exist_ok=True)
    spool_dir.mkdir(parents=True, exist_ok=True)
    # Spool files are anonymous (O_TMPFILE) on Linux; named ones are left behind only by a crash.
    for leftover in spool_dir.glob(SPOOL_PREFIX + "*"):
        leftover.unlink()

    started_at = utcnow()
    pause = AuthPause()
    writers = {f: HourlyWriter(landing, landing_name(f)) for f in feeds}
    pollers = [Poller(f, base_url, auth, client_name, writers[f], spool_dir, pause) for f in feeds]
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
            threads.append(threading.Thread(target=p.run, args=(stop, i * STAGGER_S), daemon=True,
                                            name=f"datex-{p.feed}"))
            threads[-1].start()
    while not stop.is_set():
        now = utcnow()
        for w in writers.values():
            w.rotate(now)
        error = (None if polling else "DATEX_USERNAME and DATEX_PASSWORD not set") \
            or (pause.reason if pause.remaining() > 0 else None)
        write_state(state_file, pollers, started_at, polling, error)
        stop.wait(ROTATE_CHECK_S)
    # Idle feeds end at once. One mid-download stops before writing (see poll()); give one
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
