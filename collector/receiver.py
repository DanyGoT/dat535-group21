#!/usr/bin/env python3
"""Entur SIRI pub/sub receiver.

Holds SIRI 2.0 subscriptions (ET, SX) with Entur and appends every push, unchanged,
as one JSON line to the hourly landing file:

    <landing>/siri-<feed>/date=YYYY-MM-DD/hour=HH.jsonl.part  (renamed to .jsonl when the hour ends)

Dates and hours are UTC. Heartbeats are pushes too and are stored like everything else.
No parsing, no filtering. Standard library only.

Environment:
    DAT535_LANDING_DIR      landing root (default: ./landing)
    RECEIVER_BIND           listen address (default: 127.0.0.1)
    RECEIVER_PORT           listen port (default: 8750)
    RECEIVER_PUBLIC_URL     base URL Entur can reach, e.g. https://collector.example.org
                            unset = receive only, never subscribe (local testing)
    RECEIVER_STATE_FILE     small JSON status file for the monitor (default: ./run/receiver.json)
    RECEIVER_FEEDS          comma-separated feeds (default: et,sx)
    ET_CLIENT_NAME          ET-Client-Name header and SIRI requestor (default: uis-dat535group21-collector)
    ENTUR_SUBSCRIBE_URL     default: https://api.entur.io/realtime/v1/subscribe
"""

import json
import logging
import os
import re
import signal
import sys
import threading
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from xml.sax.saxutils import escape

log = logging.getLogger("receiver")

FEED_PATHS = {"/siri/et": "et", "/siri/sx": "sx"}
MAX_BODY = 256 * 1024 * 1024        # refuse absurd bodies on a public endpoint
ROTATE_CHECK_S = 10

HEARTBEAT = timedelta(seconds=60)   # Entur clamps to 30-60 s
SILENCE_LIMIT = 3 * HEARTBEAT       # no push for this long = subscription presumed dead
RENEW_AFTER = timedelta(hours=24)   # re-subscribe periodically, long before termination
TERMINATION = timedelta(days=7)     # InitialTerminationTime
MIN_RESUBSCRIBE = timedelta(minutes=5)  # never hit /subscribe more often than this per feed
SUBSCRIBE_TIMEOUT_S = 30


def utcnow():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class HourlyWriter:
    """Appends lines to <root>/siri-<feed>/date=.../hour=HH.jsonl.part; renames on hour change."""

    def __init__(self, root, feed):
        self.dir = Path(root) / f"siri-{feed}"
        self.lock = threading.Lock()
        self.key = None
        self.fh = None
        self.finalize_stale(utcnow())

    def _part(self, key):
        date, hour = key
        return self.dir / f"date={date}" / f"hour={hour}.jsonl.part"

    @staticmethod
    def _key(now):
        return now.strftime("%Y-%m-%d"), now.strftime("%H")

    def _close(self):
        if self.fh is None:
            return
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()
        self.fh = None
        self._finalize(self._part(self.key))
        self.key = None

    @staticmethod
    def _finalize(part):
        final = part.with_name(part.name[: -len(".part")])
        if final.exists():
            # Never overwrite a closed file; leave the .part for a human to look at.
            log.error("not finalizing %s: %s already exists", part, final.name)
            return
        os.rename(part, final)
        log.info("closed %s", final)

    def finalize_stale(self, now):
        """Close .part files from earlier hours, e.g. left behind by a crash."""
        current = self._part(self._key(now))
        for part in sorted(self.dir.glob("date=*/hour=*.jsonl.part")):
            if part != current:
                self._finalize(part)

    def rotate(self, now):
        with self.lock:
            if self.key is not None and self.key != self._key(now):
                self._close()

    def append(self, line, now):
        with self.lock:
            key = self._key(now)
            if self.key != key:
                self._close()
                part = self._part(key)
                part.parent.mkdir(parents=True, exist_ok=True)
                self.fh = open(part, "a", encoding="utf-8")
                self.key = key
            self.fh.write(line)
            self.fh.flush()

    def close(self):
        """Shutdown: flush and close, but keep .part (the hour may not be over)."""
        with self.lock:
            if self.fh is not None:
                self.fh.flush()
                os.fsync(self.fh.fileno())
                self.fh.close()
                self.fh = None
                self.key = None


class Subscriptions:
    """Keeps one Entur subscription per feed alive; re-subscribes on silence or age."""

    def __init__(self, feeds, public_url, client_name, subscribe_url):
        self.public_url = public_url.rstrip("/") if public_url else None
        self.client_name = client_name
        self.subscribe_url = subscribe_url
        self.lock = threading.Lock()
        self.state = {
            f: {"id": f"{client_name}-{f}", "subscribed_at": None, "expires_at": None,
                "last_attempt": None, "last_push": None, "last_error": None}
            for f in feeds
        }

    def touch(self, feed, now):
        with self.lock:
            self.state[feed]["last_push"] = now

    def snapshot(self):
        with self.lock:
            return {f: {k: iso(v) if isinstance(v, datetime) else v for k, v in s.items()}
                    for f, s in self.state.items()}

    def _due(self, s, now):
        if s["last_attempt"] and now - s["last_attempt"] < MIN_RESUBSCRIBE:
            return False
        if s["subscribed_at"] is None:
            return True
        if now - s["subscribed_at"] > RENEW_AFTER:
            return True
        last = max(s["last_push"] or s["subscribed_at"], s["subscribed_at"])
        return now - last > SILENCE_LIMIT

    def tick(self, now):
        if not self.public_url:
            return
        for feed in self.state:
            with self.lock:
                s = self.state[feed]
                if not self._due(s, now):
                    continue
                s["last_attempt"] = now
            ok, err = self._subscribe(feed, s["id"], now)
            with self.lock:
                s["last_error"] = err
                if ok:
                    s["subscribed_at"] = now
                    s["expires_at"] = now + TERMINATION

    def _post(self, body, timeout):
        req = urllib.request.Request(
            self.subscribe_url, data=body.encode("utf-8"), method="POST",
            headers={"Content-Type": "application/xml", "ET-Client-Name": self.client_name})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")

    def _subscribe(self, feed, sub_id, now):
        body = subscription_request(feed, sub_id, f"{self.public_url}/siri/{feed}",
                                    self.client_name, now, now + TERMINATION)
        try:
            resp = self._post(body, SUBSCRIBE_TIMEOUT_S)
        except (urllib.error.URLError, OSError) as e:
            log.error("subscribe %s failed: %s", feed, e)
            return False, str(e)
        if re.search(r"<(?:\w+:)?Status>\s*true\s*</", resp):
            log.info("subscribed %s as %s until %s", feed, sub_id, iso(now + TERMINATION))
            return True, None
        log.error("subscribe %s rejected: %s", feed, resp[:500])
        return False, "rejected: " + " ".join(resp.split())[:200]

    def terminate_all(self):
        if not self.public_url:
            return
        now = utcnow()
        for s in self.state.values():
            if s["subscribed_at"] is None:
                continue
            try:
                self._post(terminate_request(s["id"], self.client_name, now), 10)
                log.info("terminated %s", s["id"])
            except (urllib.error.URLError, OSError) as e:
                log.warning("terminate %s failed: %s", s["id"], e)


def subscription_request(feed, sub_id, address, client_name, now, until):
    if feed == "et":
        inner = (
            "<EstimatedTimetableSubscriptionRequest>"
            f"<SubscriberRef>{escape(client_name)}</SubscriberRef>"
            f"<SubscriptionIdentifier>{escape(sub_id)}</SubscriptionIdentifier>"
            f"<InitialTerminationTime>{iso(until)}</InitialTerminationTime>"
            '<EstimatedTimetableRequest version="2.0">'
            f"<RequestTimestamp>{iso(now)}</RequestTimestamp>"
            "</EstimatedTimetableRequest>"
            "<ChangeBeforeUpdates>PT10S</ChangeBeforeUpdates>"
            "</EstimatedTimetableSubscriptionRequest>")
    elif feed == "sx":
        inner = (
            "<SituationExchangeSubscriptionRequest>"
            f"<SubscriberRef>{escape(client_name)}</SubscriberRef>"
            f"<SubscriptionIdentifier>{escape(sub_id)}</SubscriptionIdentifier>"
            f"<InitialTerminationTime>{iso(until)}</InitialTerminationTime>"
            '<SituationExchangeRequest version="2.0">'
            f"<RequestTimestamp>{iso(now)}</RequestTimestamp>"
            "</SituationExchangeRequest>"
            "</SituationExchangeSubscriptionRequest>")
    else:
        raise ValueError(feed)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Siri version="2.0" xmlns="http://www.siri.org.uk/siri">'
        "<SubscriptionRequest>"
        f"<RequestTimestamp>{iso(now)}</RequestTimestamp>"
        f"<Address>{escape(address)}</Address>"
        f"<RequestorRef>{escape(client_name)}</RequestorRef>"
        f"<MessageIdentifier>{uuid.uuid4()}</MessageIdentifier>"
        f"<SubscriptionContext><HeartbeatInterval>PT{int(HEARTBEAT.total_seconds())}S"
        "</HeartbeatInterval></SubscriptionContext>"
        f"{inner}"
        "</SubscriptionRequest></Siri>")


def terminate_request(sub_id, client_name, now):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Siri version="2.0" xmlns="http://www.siri.org.uk/siri">'
        "<TerminateSubscriptionRequest>"
        f"<RequestTimestamp>{iso(now)}</RequestTimestamp>"
        f"<RequestorRef>{escape(client_name)}</RequestorRef>"
        f"<MessageIdentifier>{uuid.uuid4()}</MessageIdentifier>"
        f"<SubscriptionRef>{escape(sub_id)}</SubscriptionRef>"
        "</TerminateSubscriptionRequest></Siri>")


class Handler(BaseHTTPRequestHandler):
    server_version = "receiver"
    sys_version = ""
    protocol_version = "HTTP/1.1"
    writers = {}
    subs: "Subscriptions | None" = None

    def log_message(self, format, *args):
        log.debug("%s %s", self.address_string(), format % args)

    def _reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _read_body(self):
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            parts, total = [], 0
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass  # trailers
                    return b"".join(parts)
                total += size
                if total > MAX_BODY:
                    raise ValueError("body too large")
                parts.append(self.rfile.read(size))
                self.rfile.readline()
        length = int(self.headers.get("Content-Length", "0"))
        if length > MAX_BODY:
            raise ValueError("body too large")
        return self.rfile.read(length)

    def do_HEAD(self):
        # Entur probes the address with HEAD before re-accepting a failed subscription.
        self._reply(200 if self.path in FEED_PATHS else 404)

    def do_GET(self):
        self._reply(404)

    def do_POST(self):
        feed = FEED_PATHS.get(self.path)
        if feed is None or feed not in self.writers:
            self.close_connection = True
            return self._reply(404)
        try:
            payload = self._read_body().decode("utf-8")
        except (ValueError, UnicodeDecodeError) as e:
            log.error("bad push on %s: %s", self.path, e)
            self.close_connection = True
            return self._reply(400)
        now = utcnow()
        line = json.dumps({"received_at": iso(now), "endpoint": feed, "payload": payload},
                          ensure_ascii=False) + "\n"
        self.writers[feed].append(line, now)
        if self.subs:
            self.subs.touch(feed, now)
        self._reply(200)


def write_state(path, subs, started_at):
    state = {"pid": os.getpid(), "started_at": iso(started_at), "updated_at": iso(utcnow()),
             "subscribing": bool(subs.public_url), "feeds": subs.snapshot()}
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    os.replace(tmp, path)


def main():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    landing = Path(os.environ.get("DAT535_LANDING_DIR", "landing"))
    bind = os.environ.get("RECEIVER_BIND", "127.0.0.1")
    port = int(os.environ.get("RECEIVER_PORT", "8750"))
    feeds = [f.strip() for f in os.environ.get("RECEIVER_FEEDS", "et,sx").split(",") if f.strip()]
    state_file = Path(os.environ.get("RECEIVER_STATE_FILE", "run/receiver.json"))
    client_name = os.environ.get("ET_CLIENT_NAME", "uis-dat535group21-collector")

    for f in feeds:
        if f not in FEED_PATHS.values():
            sys.exit(f"unknown feed: {f}")
    state_file.parent.mkdir(parents=True, exist_ok=True)

    started_at = utcnow()
    Handler.writers = {f: HourlyWriter(landing, f) for f in feeds}
    subs = Subscriptions(feeds, os.environ.get("RECEIVER_PUBLIC_URL"), client_name,
                         os.environ.get("ENTUR_SUBSCRIBE_URL",
                                        "https://api.entur.io/realtime/v1/subscribe"))
    Handler.subs = subs
    server = ThreadingHTTPServer((bind, port), Handler)
    server.daemon_threads = True
    stop = threading.Event()

    def background():
        while not stop.is_set():
            now = utcnow()
            for w in Handler.writers.values():
                w.rotate(now)
            subs.tick(now)
            write_state(state_file, subs, started_at)
            stop.wait(ROTATE_CHECK_S)

    def shutdown(signum, _frame):
        log.info("signal %s, shutting down", signum)
        stop.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    log.info("listening on %s:%d, feeds=%s, landing=%s, subscribing=%s",
             bind, port, ",".join(feeds), landing, bool(subs.public_url))
    bg = threading.Thread(target=background, daemon=True)
    bg.start()
    server.serve_forever()
    bg.join(timeout=5)
    subs.terminate_all()
    for w in Handler.writers.values():
        w.close()
    try:
        state_file.unlink()
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    main()
