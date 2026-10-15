"""Local tests for receiver and monitor. No network beyond localhost; never calls Entur.

Run from the repo root:  python3 -m unittest discover -s collector/tests -v
"""

import http.client
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
COLLECTOR = HERE.parent
sys.path.insert(0, str(COLLECTOR))

import receiver  # noqa: E402

ET_PUSH = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Siri version="2.0" xmlns="http://www.siri.org.uk/siri">
    <ServiceDelivery>
        <ResponseTimestamp>2026-10-15T10:00:00+02:00</ResponseTimestamp>
        <ProducerRef>ENT</ProducerRef>
        <EstimatedTimetableDelivery version="2.0">
            <EstimatedJourneyVersionFrame><!-- æøå --></EstimatedJourneyVersionFrame>
        </EstimatedTimetableDelivery>
    </ServiceDelivery>
</Siri>
"""


def at(h, m):
    return datetime(2026, 10, 15, h, m, tzinfo=timezone.utc)


class Recorder(BaseHTTPRequestHandler):
    """Fake upstream (Entur /subscribe or ntfy): records requests, answers with `reply`."""
    reply = b""
    requests = []

    def log_message(self, format, *args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        type(self).requests.append((self.path, self.headers, body.decode("utf-8")))
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.reply)))
        self.end_headers()
        self.wfile.write(self.reply)


def serve(handler):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


class WriterTest(unittest.TestCase):
    def test_hourly_rotation_and_stale_finalize(self):
        with tempfile.TemporaryDirectory() as d:
            w = receiver.HourlyWriter(d, "et")
            w.append("a\n", at(10, 59))
            w.append("b\n", at(10, 59))
            base = Path(d) / "siri-et" / "date=2026-10-15"
            self.assertTrue((base / "hour=10.jsonl.part").exists())
            w.rotate(at(11, 0))
            self.assertEqual((base / "hour=10.jsonl").read_text(), "a\nb\n")
            self.assertFalse((base / "hour=10.jsonl.part").exists())
            w.append("c\n", at(11, 1))
            w.close()  # shutdown keeps .part
            self.assertTrue((base / "hour=11.jsonl.part").exists())
            # Restart later: stale .part from an earlier hour gets closed
            receiver.HourlyWriter.finalize_stale(receiver.HourlyWriter(d, "et"), at(12, 0))
            self.assertEqual((base / "hour=11.jsonl").read_text(), "c\n")

    def test_never_overwrites_closed_file(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d) / "siri-et" / "date=2026-10-15"
            base.mkdir(parents=True)
            (base / "hour=09.jsonl").write_text("old\n")
            (base / "hour=09.jsonl.part").write_text("new\n")
            receiver.HourlyWriter(d, "et")
            self.assertEqual((base / "hour=09.jsonl").read_text(), "old\n")
            self.assertTrue((base / "hour=09.jsonl.part").exists())


class ReceiverHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        receiver.Handler.writers = {"et": receiver.HourlyWriter(self.tmp.name, "et")}
        receiver.Handler.subs = None
        self.srv = serve(receiver.Handler)
        self.port = self.srv.server_address[1]

    def tearDown(self):
        self.srv.shutdown()
        receiver.Handler.writers["et"].close()
        self.tmp.cleanup()

    def request(self, method, path, body=None, headers=None, chunked=False):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        if chunked:
            c.putrequest(method, path)
            c.putheader("Transfer-Encoding", "chunked")
            c.putheader("Content-Type", "application/xml")
            c.endheaders()
            data = body.encode("utf-8")
            for i in range(0, len(data), 100):
                piece = data[i:i + 100]
                c.send(b"%x\r\n%s\r\n" % (len(piece), piece))
            c.send(b"0\r\n\r\n")
        else:
            c.request(method, path, body=body.encode("utf-8") if body else None, headers=headers or {})
        r = c.getresponse()
        r.read()
        c.close()
        return r.status

    def lines(self):
        files = list(Path(self.tmp.name).glob("siri-et/date=*/hour=*.jsonl.part"))
        self.assertEqual(len(files), 1)
        return [json.loads(l) for l in files[0].read_text(encoding="utf-8").splitlines()]

    def test_push_stored_unchanged(self):
        self.assertEqual(self.request("POST", "/siri/et", ET_PUSH, {"Content-Type": "application/xml"}), 200)
        self.assertEqual(self.request("POST", "/siri/et", ET_PUSH, chunked=True), 200)
        lines = self.lines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            self.assertEqual(set(line), {"received_at", "endpoint", "payload"})
            self.assertEqual(line["endpoint"], "et")
            self.assertEqual(line["payload"], ET_PUSH)
            self.assertTrue(line["received_at"].endswith("Z"))

    def test_other_methods_and_paths(self):
        self.assertEqual(self.request("HEAD", "/siri/et"), 200)
        self.assertEqual(self.request("GET", "/siri/et"), 404)
        self.assertEqual(self.request("POST", "/siri/sx", "x"), 404)  # feed not enabled
        self.assertEqual(self.request("POST", "/", "x"), 404)


class SubscribeTest(unittest.TestCase):
    def test_subscribe_against_fake_entur(self):
        Recorder.requests = []
        Recorder.reply = (b'<Siri xmlns="http://www.siri.org.uk/siri"><SubscriptionResponse><ResponseStatus>'
                          b'<Status>true</Status></ResponseStatus></SubscriptionResponse></Siri>')
        srv = serve(Recorder)
        try:
            subs = receiver.Subscriptions(["et", "sx"], "https://example.org/", "test-client",
                                          f"http://127.0.0.1:{srv.server_address[1]}/subscribe")
            now = at(10, 0)
            subs.tick(now)
            self.assertEqual(len(Recorder.requests), 2)
            _, headers, body = Recorder.requests[0]
            self.assertEqual(headers["ET-Client-Name"], "test-client")
            self.assertIn("<Address>https://example.org/siri/et</Address>", body)
            self.assertIn("<HeartbeatInterval>PT60S</HeartbeatInterval>", body)
            self.assertIn("<SubscriptionIdentifier>test-client-et</SubscriptionIdentifier>", body)
            self.assertIn("SituationExchangeSubscriptionRequest", Recorder.requests[1][2])

            subs.touch("et", at(10, 2))
            subs.touch("sx", at(10, 2))
            subs.tick(at(10, 4))  # heartbeats flowing: nothing to do
            self.assertEqual(len(Recorder.requests), 2)
            subs.touch("sx", at(10, 9))
            subs.tick(at(10, 10))  # et silent > 3 min -> re-subscribe et only
            self.assertEqual(len(Recorder.requests), 3)
            self.assertIn("test-client-et", Recorder.requests[2][2])
        finally:
            srv.shutdown()

    def test_no_public_url_never_subscribes(self):
        subs = receiver.Subscriptions(["et"], None, "c", "http://127.0.0.1:9/never")
        subs.tick(at(10, 0))
        self.assertIsNone(subs.state["et"]["last_attempt"])


class MonitorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.landing = self.tmp / "landing"
        now = datetime.now(timezone.utc)
        self.part = self.landing / "siri-et" / f"date={now:%Y-%m-%d}" / f"hour={now:%H}.jsonl.part"
        self.part.parent.mkdir(parents=True)
        self.part.write_text("x\n")
        self.state = self.tmp / "receiver.json"
        self.state.write_text(json.dumps({"pid": os.getpid(), "started_at": "2026-10-15T08:00:00Z",
                                          "subscribing": False, "feeds": {"et": {}}}))
        Recorder.requests = []
        Recorder.reply = b"{}"
        self.ntfy = serve(Recorder)

    def tearDown(self):
        self.ntfy.shutdown()
        shutil.rmtree(self.tmp)

    def run_monitor(self):
        env = dict(os.environ, DAT535_LANDING_DIR=str(self.landing), RECEIVER_STATE_FILE=str(self.state),
                   RECEIVER_FEEDS="et", MONITOR_STATUS_DIR=str(self.tmp / "status"),
                   MONITOR_DISK_MAX_PCT="100", NTFY_TOPIC="topic",
                   NTFY_SERVER=f"http://127.0.0.1:{self.ntfy.server_address[1]}")
        return subprocess.run([sys.executable, str(COLLECTOR / "monitor.py")], env=env,
                              capture_output=True, text=True, timeout=30)

    def test_state_changes_and_notifications(self):
        landing_before = sorted(p.relative_to(self.landing) for p in self.landing.rglob("*"))

        r = self.run_monitor()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(Recorder.requests, [])  # first run OK: no notification

        r = self.run_monitor()  # no growth since last check
        self.assertEqual(r.returncode, 1)
        self.assertIn("has not grown", r.stdout)
        self.assertEqual(len(Recorder.requests), 1)
        self.assertIn("FAIL", Recorder.requests[0][1]["Title"])

        self.state.unlink()  # receiver gone too: still FAIL, no second notification
        r = self.run_monitor()
        self.assertEqual(r.returncode, 1)
        self.assertIn("receiver not running", r.stdout)
        self.assertEqual(len(Recorder.requests), 1)

        self.state.write_text(json.dumps({"pid": os.getpid(), "started_at": "2026-10-15T08:00:00Z"}))
        with open(self.part, "a") as f:
            f.write("y\n")
        r = self.run_monitor()
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertEqual(len(Recorder.requests), 2)
        self.assertIn("OK", Recorder.requests[1][1]["Title"])

        page = (self.tmp / "status" / "index.html").read_text()
        self.assertIn("DAT535 COLLECTOR", page)
        self.assertNotIn("<form", page.lower())
        self.assertEqual(landing_before, sorted(p.relative_to(self.landing) for p in self.landing.rglob("*")))

    def test_stale_part(self):
        old = time.time() - 600
        os.utime(self.part, (old, old))
        r = self.run_monitor()
        self.assertEqual(r.returncode, 1)
        self.assertIn("not modified for", r.stdout)


if __name__ == "__main__":
    unittest.main()
