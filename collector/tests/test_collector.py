"""Local tests for receiver and monitor. No network beyond localhost; never calls Entur.

Run from the repo root:  python3 -m unittest discover -s collector/tests -v
"""

import gzip
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
import unittest.mock
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
            w.pending.join()
            self.assertEqual(gzip.decompress((base / "hour=10.jsonl.gz").read_bytes()), b"a\nb\n")
            self.assertFalse((base / "hour=10.jsonl.part").exists())
            self.assertEqual(sorted(p.name for p in base.iterdir()), ["hour=10.jsonl.gz"])
            w.append("c\n", at(11, 1))
            w.close()  # shutdown keeps .part
            self.assertTrue((base / "hour=11.jsonl.part").exists())
            # Restart later: stale .part from an earlier hour gets compressed
            w2 = receiver.HourlyWriter(d, "et")
            w2.finalize_stale(at(12, 0))
            w2.pending.join()
            self.assertEqual(gzip.decompress((base / "hour=11.jsonl.gz").read_bytes()), b"c\n")
            self.assertFalse((base / "hour=11.jsonl.part").exists())

    def test_restart_within_hour_without_pushes(self):
        # Restart mid-hour, then silence: the adopted .part must still close at hour end.
        with tempfile.TemporaryDirectory() as d:
            w = receiver.HourlyWriter(d, "et")
            w.append("a\n", at(12, 10))
            w.close()
            base = Path(d) / "siri-et" / "date=2026-10-15"
            w2 = receiver.HourlyWriter(d, "et")
            w2.adopt_current(at(12, 37))
            w2.rotate(at(13, 0))
            w2.pending.join()
            self.assertEqual(sorted(p.name for p in base.iterdir()), ["hour=12.jsonl.gz"])
            self.assertEqual(gzip.decompress((base / "hour=12.jsonl.gz").read_bytes()), b"a\n")

    def test_never_overwrites_closed_file(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d) / "siri-et" / "date=2026-10-15"
            base.mkdir(parents=True)
            (base / "hour=08.jsonl").write_text("old\n")          # closed by an older version
            (base / "hour=08.jsonl.part").write_text("new\n")
            (base / "hour=09.jsonl.gz").write_bytes(gzip.compress(b"old\n"))
            (base / "hour=09.jsonl.part").write_text("new\n")
            receiver.HourlyWriter(d, "et").pending.join()
            self.assertEqual((base / "hour=08.jsonl").read_text(), "old\n")
            self.assertTrue((base / "hour=08.jsonl.part").exists())
            self.assertEqual(gzip.decompress((base / "hour=09.jsonl.gz").read_bytes()), b"old\n")
            self.assertTrue((base / "hour=09.jsonl.part").exists())

    def test_leftover_part_after_crash(self):
        # Crash after the .gz was renamed into place but before the .part was removed.
        with tempfile.TemporaryDirectory() as d:
            base = Path(d) / "siri-et" / "date=2026-10-15"
            base.mkdir(parents=True)
            (base / "hour=09.jsonl.gz").write_bytes(gzip.compress(b"same\n"))
            (base / "hour=09.jsonl.part").write_text("same\n")
            receiver.HourlyWriter(d, "et").pending.join()
            self.assertEqual(sorted(p.name for p in base.iterdir()), ["hour=09.jsonl.gz"])


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
        self.feeds = "et"

    def tearDown(self):
        self.ntfy.shutdown()
        shutil.rmtree(self.tmp)

    def run_monitor(self):
        env = dict(os.environ, DAT535_LANDING_DIR=str(self.landing), RECEIVER_STATE_FILE=str(self.state),
                   MONITOR_FEEDS=self.feeds, MONITOR_STATUS_DIR=str(self.tmp / "status"),
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
        self.assertIn("et: no pushes received for", r.stdout)
        self.assertEqual(len(Recorder.requests), 1)
        self.assertEqual(Recorder.requests[0][1]["Title"], "Collector FAIL")
        self.assertRegex(Recorder.requests[0][2], r"^et: no data for \d+s$")

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
        self.assertEqual(Recorder.requests[1][1]["Title"], "Collector OK")
        self.assertTrue(Recorder.requests[1][2].startswith("recovered after "))

        page = (self.tmp / "status" / "index.html").read_text()
        self.assertIn("DAT535 COLLECTOR", page)
        self.assertNotIn("<form", page.lower())
        self.assertEqual(landing_before, sorted(p.relative_to(self.landing) for p in self.landing.rglob("*")))

    def test_vehicles_feed(self):
        now = datetime.now(timezone.utc)
        part = self.landing / "vehicles" / f"date={now:%Y-%m-%d}" / f"hour={now:%H}.jsonl.part"
        self.feeds = "et,vehicles"
        r = self.run_monitor()
        self.assertEqual(r.returncode, 1)
        self.assertIn("vehicles: nothing in the landing zone yet", r.stdout)
        part.parent.mkdir(parents=True)
        part.write_text("x\n")
        self.part.write_text("x\ny\n")
        r = self.run_monitor()
        self.assertEqual(r.returncode, 0, r.stdout)
        status = json.loads((self.tmp / "status" / "status.json").read_text())
        self.assertTrue(status["checks"]["feed:vehicles"]["ok"])
        self.assertIn('<span class="name">vehicles</span>', (self.tmp / "status" / "index.html").read_text())

    def test_default_feeds(self):
        env = {k: v for k, v in os.environ.items() if k not in ("MONITOR_FEEDS", "RECEIVER_FEEDS")}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            import monitor
            self.assertEqual(monitor.feeds_from_env(), ["et", "sx", "vehicles"])
            os.environ["RECEIVER_FEEDS"] = "et"
            self.assertEqual(monitor.feeds_from_env(), ["et", "vehicles"])

    def test_stale_part(self):
        old = time.time() - 600
        os.utime(self.part, (old, old))
        r = self.run_monitor()
        self.assertEqual(r.returncode, 1)
        self.assertIn("et: no pushes received for 10m", r.stdout)

    def test_hour_turned_before_next_push(self):
        # The hour just closed (.part gzipped) and the quiet feed has not pushed yet: not a failure.
        r = self.run_monitor()
        self.assertEqual(r.returncode, 0, r.stdout)
        closed = self.part.with_name(self.part.name.replace(".part", ".gz"))
        self.part.rename(closed)
        r = self.run_monitor()
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertEqual(Recorder.requests, [])
        r = self.run_monitor()  # still nothing a check later: now it is a failure
        self.assertEqual(r.returncode, 1)
        self.assertIn("last write to", r.stdout)

    def test_all_feeds_stopped_hint(self):
        self.feeds = "et,sx"
        now = datetime.now(timezone.utc)
        sx = self.landing / "siri-sx" / f"date={now:%Y-%m-%d}" / f"hour={now:%H}.jsonl.part"
        sx.parent.mkdir(parents=True)
        sx.write_text("x\n")
        self.run_monitor()
        self.run_monitor()
        self.assertEqual(len(Recorder.requests), 1)
        self.assertIn("all feeds stopped together", Recorder.requests[0][2])


class ExporterTest(unittest.TestCase):
    def test_metrics(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            et = d / "landing" / "siri-et"
            (et / "date=2026-10-14").mkdir(parents=True)
            (et / "date=2026-10-15").mkdir()
            (et / "date=2026-10-14" / "hour=23.jsonl").write_bytes(b"x" * 10)        # closed before gzip
            (et / "date=2026-10-15" / "hour=00.jsonl.gz").write_bytes(b"x" * 100)
            (et / "date=2026-10-15" / "hour=01.jsonl.gz").write_bytes(b"x" * 200)
            (et / "date=2026-10-15" / "hour=02.jsonl.gz.tmp").write_bytes(b"x" * 999)  # compressing
            (et / "date=2026-10-15" / "hour=02.jsonl.part").write_bytes(b"x" * 999)
            (et / "date=2026-10-15" / "hour=03.jsonl.part").write_bytes(b"x" * 50)
            sx = d / "landing" / "siri-sx" / "date=2026-10-15"
            sx.mkdir(parents=True)
            (sx / "hour=01.jsonl.gz").write_bytes(b"x" * 7)
            veh = d / "landing" / "vehicles" / "date=2026-10-15"
            veh.mkdir(parents=True)
            (veh / "hour=01.jsonl.gz").write_bytes(b"x" * 11)
            (veh / "hour=02.jsonl.part").write_bytes(b"x" * 5)
            state = d / "receiver.json"
            state.write_text(json.dumps({
                "pid": os.getpid(), "started_at": "2026-10-15T00:00:00Z", "subscribing": True,
                "feeds": {"et": {"last_push": "2026-10-15T03:00:00Z", "expires_at": "2026-10-22T00:00:00Z",
                                 "last_error": None},
                          "sx": {"last_push": None, "last_error": "rejected"}}}))
            (d / "status").mkdir()
            (d / "status" / "status.json").write_text(json.dumps({
                "ok": False, "checked_at": "2026-10-15T03:00:00Z",
                "checks": {"receiver": {"ok": True}, "disk": {"ok": False}}}))

            import exporter
            text = exporter.Collector(str(d / "landing"), str(state), ["et", "sx", "vehicles"],
                                      str(d / "status")).collect()

        m = {}
        for line in text.splitlines():
            if line and not line.startswith("#"):
                key, value = line.rsplit(" ", 1)
                m[key] = float(value)
        self.assertEqual(m['dat535_landing_closed_bytes{feed="et"}'], 310)
        self.assertEqual(m['dat535_landing_closed_files{feed="et"}'], 3)
        self.assertEqual(m['dat535_landing_closed_bytes{feed="sx"}'], 7)
        self.assertEqual(m['dat535_landing_open_bytes{feed="et"}'], 50)
        self.assertEqual(m['dat535_landing_pending_parts{feed="et"}'], 1)
        self.assertNotIn('dat535_landing_open_bytes{feed="sx"}', m)
        self.assertEqual(m['dat535_landing_closed_bytes{feed="vehicles"}'], 11)
        self.assertEqual(m['dat535_landing_open_bytes{feed="vehicles"}'], 5)
        self.assertEqual(m['dat535_landing_day_bytes{feed="et",date="2026-10-15"}'], 300)
        self.assertEqual(m['dat535_landing_day_hours{feed="et",date="2026-10-15"}'], 2)
        self.assertEqual(m['dat535_landing_day_hours{feed="et",date="2026-10-14"}'], 1)
        self.assertEqual(m['dat535_landing_first_hour_timestamp_seconds{feed="et"}'],
                         datetime(2026, 10, 14, 23, tzinfo=timezone.utc).timestamp())
        self.assertEqual(m["dat535_receiver_up"], 1)
        self.assertEqual(m['dat535_subscription_last_push_timestamp_seconds{feed="et"}'],
                         at(3, 0).timestamp())
        self.assertNotIn('dat535_subscription_last_push_timestamp_seconds{feed="sx"}', m)
        self.assertEqual(m['dat535_subscription_error{feed="sx"}'], 1)
        self.assertEqual(m["dat535_monitor_ok"], 0)
        self.assertEqual(m['dat535_monitor_check_ok{check="disk"}'], 0)
        self.assertIn('dat535_filesystem_free_bytes{volume="landing"}', m)


class FakeGrafana(BaseHTTPRequestHandler):
    """Echoes method, path, body and a few headers back as JSON."""

    def log_message(self, format, *args):
        pass

    def reply(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        out = json.dumps({"method": self.command, "path": self.path, "body": body.decode(),
                          "host": self.headers["Host"], "xff": self.headers["X-Forwarded-For"],
                          "connection": self.headers["Connection"]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(out)

    do_GET = do_POST = do_HEAD = reply


class StatusServerTest(unittest.TestCase):
    def setUp(self):
        import serve_status
        from functools import partial
        self.tmp = tempfile.TemporaryDirectory()
        Path(self.tmp.name, "index.html").write_text("<p>main</p>")
        Path(self.tmp.name, ".secret").write_text("no")
        self.grafana = serve(FakeGrafana)
        serve_status.StatusHandler.upstream = serve_status.urllib.parse.urlsplit(
            f"http://127.0.0.1:{self.grafana.server_address[1]}")
        self.srv = serve(partial(serve_status.StatusHandler, directory=self.tmp.name))
        self.port = self.srv.server_address[1]

    def tearDown(self):
        self.srv.shutdown()
        self.grafana.shutdown()
        self.tmp.cleanup()

    def request(self, method, path, body=None):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        c.request(method, path, body=body)
        r = c.getresponse()
        data = r.read()
        c.close()
        return r, data

    def test_static_page(self):
        r, data = self.request("GET", "/")
        self.assertEqual(r.status, 200)
        self.assertEqual(data, b"<p>main</p>")
        self.assertIn("frame-src 'self'", r.getheader("Content-Security-Policy"))
        self.assertEqual(self.request("GET", "/.secret")[0].status, 404)
        self.assertEqual(self.request("POST", "/", "x")[0].status, 501)
        self.assertEqual(self.request("GET", "/grafanax")[0].status, 404)  # not proxied

    def test_proxy(self):
        r, data = self.request("GET", "/grafana/d/abc?orgId=1")
        self.assertEqual(r.status, 200)
        echo = json.loads(data)
        self.assertEqual(echo["method"], "GET")
        self.assertEqual(echo["path"], "/grafana/d/abc?orgId=1")
        self.assertEqual(echo["host"], f"127.0.0.1:{self.port}")  # browser's Host, for Grafana's CSRF check
        self.assertEqual(echo["xff"], "127.0.0.1")
        self.assertIsNone(r.getheader("Content-Security-Policy"))  # Grafana's headers, not ours

        r, data = self.request("POST", "/grafana/api/ds/query", '{"queries": []}')
        self.assertEqual(json.loads(data)["body"], '{"queries": []}')

        r, data = self.request("HEAD", "/grafana/")
        self.assertEqual((r.status, data), (200, b""))

    def test_grafana_down(self):
        self.grafana.shutdown()
        self.grafana.server_close()
        self.assertEqual(self.request("GET", "/grafana/")[0].status, 502)


if __name__ == "__main__":
    unittest.main()
