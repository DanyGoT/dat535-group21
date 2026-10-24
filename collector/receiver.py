#!/usr/bin/env python3
"""Entur SIRI pub/sub receiver.

Holds SIRI 2.0 subscriptions (ET, SX) with Entur and appends every push, unchanged,
as one JSON line to the hourly landing file:

    <landing>/siri-<feed>/date=YYYY-MM-DD/hour=HH.jsonl.part  (gzipped to .jsonl.gz when the hour ends)

Dates and hours are UTC. Heartbeats are pushes too and are stored like everything else.
No parsing, no filtering. Standard library only.

Push bodies are streamed to an anonymous spool file (never held in memory whole), checked
to be complete UTF-8, and only then appended to the .part. The spool lives outside the
landing tree so nothing but finished lines ever appears there.

Environment:
    DAT535_LANDING_DIR      landing root (default: ./landing)
    RECEIVER_SPOOL_DIR      spool for pushes being received; same filesystem as the landing
                            root, outside it (default: <landing>/../receiver-spool)
    RECEIVER_BIND           listen address (default: 127.0.0.1)
    RECEIVER_PORT           listen port (default: 8750)
    RECEIVER_PUBLIC_URL     base URL Entur can reach, e.g. https://collector.example.org
                            unset = receive only, never subscribe (local testing)
    RECEIVER_STATE_FILE     small JSON status file for the monitor (default: ./run/receiver.json)
    RECEIVER_FEEDS          comma-separated feeds (default: et,sx)
    ET_CLIENT_NAME          ET-Client-Name header and SIRI requestor (default: uis-dat535group21-collector)
    ENTUR_SUBSCRIBE_URL     default: https://api.entur.io/realtime/v1/subscribe
"""

import codecs
import json
import logging
import os
import re
import signal
import sys
import tempfile
import threading
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from xml.sax.saxutils import escape

from hourly import HourlyWriter, iso, utcnow

log = logging.getLogger("receiver")

FEED_PATHS = {"/siri/et": "et", "/siri/sx": "sx"}
MAX_BODY = 256 * 1024 * 1024        # refuse absurd bodies on a public endpoint
BODY_CHUNK = 1 << 20                # pushes are streamed through memory this much at a time
MAX_LINE = 8192                     # chunk-size and trailer lines
SPOOL_PREFIX = "push-"
ROTATE_CHECK_S = 10

HEARTBEAT = timedelta(seconds=60)   # Entur clamps to 30-60 s
SILENCE_LIMIT = 3 * HEARTBEAT       # no push for this long = subscription presumed dead
RENEW_AFTER = timedelta(hours=24)   # re-subscribe periodically, long before termination
TERMINATION = timedelta(days=7)     # InitialTerminationTime
MIN_RESUBSCRIBE = timedelta(minutes=5)  # never hit /subscribe more often than this per feed
SUBSCRIBE_TIMEOUT_S = 30


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


class Aborted(Exception):
    """The client closed the connection before the whole body arrived."""


class Handler(BaseHTTPRequestHandler):
    server_version = "receiver"
    sys_version = ""
    protocol_version = "HTTP/1.1"
    writers = {}
    subs: "Subscriptions | None" = None
    spool_dir = None                    # None = system temp dir (tests); main() sets it

    def log_message(self, format, *args):
        log.debug("%s %s", self.address_string(), format % args)

    def _reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def _line(self, what, got):
        line = self.rfile.readline(MAX_LINE)
        if not line:
            raise Aborted(f"connection closed before {what} ({got} body bytes received)")
        if not line.endswith(b"\n"):
            raise ValueError(f"{what} line too long")
        return line

    def _spool_body(self, spool):
        """Stream the body (Content-Length or chunked) into `spool`, already escaped as the
        inside of a JSON string, so the append is a plain copy.

        Raises Aborted on EOF mid-body, ValueError (incl. UnicodeDecodeError) on a body
        we refuse. Returns the body size in bytes.
        """
        decoder = codecs.getincrementaldecoder("utf-8")()
        got = 0

        def take(n):
            nonlocal got
            while n:
                data = self.rfile.read(min(n, BODY_CHUNK))
                if not data:
                    raise Aborted(f"connection closed mid-body ({got} body bytes received)")
                got += len(data)
                n -= len(data)
                # json.dumps escapes per character, so escaping piecewise gives the same
                # bytes as escaping the whole payload; the decoder holds back split sequences.
                spool.write(json.dumps(decoder.decode(data), ensure_ascii=False)[1:-1]
                            .encode("utf-8"))

        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            while True:
                line = self._line("chunk size", got)
                size = line.split(b";")[0].strip()
                if not re.fullmatch(rb"[0-9A-Fa-f]+", size):
                    raise ValueError(f"bad chunk size line {line[:40]!r}")
                size = int(size, 16)
                if size == 0:
                    break
                if got + size > MAX_BODY:
                    raise ValueError("body too large")
                take(size)
                if self._line("end of chunk", got) not in (b"\r\n", b"\n"):
                    raise ValueError("chunk data not followed by CRLF")
            # Trailers up to the blank line. The body is complete already, so a client
            # that closes here instead of sending the final CRLF still counts.
            while self.rfile.readline(MAX_LINE) not in (b"\r\n", b"\n", b""):
                pass
        else:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 0:
                raise ValueError("negative Content-Length")
            if length > MAX_BODY:
                raise ValueError("body too large")
            take(length)
        decoder.decode(b"", final=True)   # truncated UTF-8 sequence at the end
        return got

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
        with tempfile.TemporaryFile(dir=self.spool_dir, prefix=SPOOL_PREFIX) as spool:
            try:
                self._spool_body(spool)
            except Aborted as e:
                log.warning("aborted push on %s from %s (Transfer-Encoding=%s, "
                            "Content-Length=%s): %s", self.path, self.client_address[0],
                            self.headers.get("Transfer-Encoding"),
                            self.headers.get("Content-Length"), e)
                self.close_connection = True
                return
            except ValueError as e:
                log.error("bad push on %s: %s", self.path, e)
                self.close_connection = True
                return self._reply(400)
            now = utcnow()
            # Same bytes as json.dumps({..., "payload": payload}, ensure_ascii=False) + "\n".
            head = json.dumps({"received_at": iso(now), "endpoint": feed, "payload": ""},
                              ensure_ascii=False)[:-2]
            spool.seek(0)
            self.writers[feed].append_spooled(head, spool, '"}\n', now)
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
    spool_dir = Path(os.environ.get("RECEIVER_SPOOL_DIR",
                                    landing.absolute().parent / "receiver-spool"))
    bind = os.environ.get("RECEIVER_BIND", "127.0.0.1")
    port = int(os.environ.get("RECEIVER_PORT", "8750"))
    feeds = [f.strip() for f in os.environ.get("RECEIVER_FEEDS", "et,sx").split(",") if f.strip()]
    state_file = Path(os.environ.get("RECEIVER_STATE_FILE", "run/receiver.json"))
    client_name = os.environ.get("ET_CLIENT_NAME", "uis-dat535group21-collector")

    for f in feeds:
        if f not in FEED_PATHS.values():
            sys.exit(f"unknown feed: {f}")
    state_file.parent.mkdir(parents=True, exist_ok=True)
    spool_dir.mkdir(parents=True, exist_ok=True)
    # Spool files are anonymous (O_TMPFILE) on Linux; named ones only exist where that is
    # unsupported, and are left behind only by a crash.
    for leftover in spool_dir.glob(SPOOL_PREFIX + "*"):
        leftover.unlink()
        log.info("removed leftover spool file %s", leftover)

    started_at = utcnow()
    Handler.writers = {f: HourlyWriter(landing, f"siri-{f}") for f in feeds}
    subs = Subscriptions(feeds, os.environ.get("RECEIVER_PUBLIC_URL"), client_name,
                         os.environ.get("ENTUR_SUBSCRIBE_URL",
                                        "https://api.entur.io/realtime/v1/subscribe"))
    Handler.subs = subs
    Handler.spool_dir = spool_dir
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
