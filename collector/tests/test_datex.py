"""Local tests for the DATEX poller against a fake NPRA server. No network beyond localhost;
never calls Vegvesen.

Run from the repo root:  python3 -m unittest discover -s collector/tests -v
"""

import base64
import contextlib
import gzip
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import unittest.mock
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

HERE = Path(__file__).resolve().parent
COLLECTOR = HERE.parent
sys.path.insert(0, str(COLLECTOR))

import datex  # noqa: E402
import hourly  # noqa: E402

XML = ('<?xml version="1.0" encoding="UTF-8"?>\n<d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload">'
       '<sit:situation id="NPRA_HBT_1">"Bergen–Åsane" æøå \\ \t</sit:situation></d2:payload>\n')
LAST_MODIFIED = "Fri, 23 Oct 2026 13:20:00 GMT"
AUTH = "Basic " + base64.b64encode(b"user:secret").decode()


class FakeNPRA(BaseHTTPRequestHandler):
    """Answers like NPRA's pull service; per-test behaviour in class attributes."""
    body = XML.encode("utf-8")
    last_modified = LAST_MODIFIED
    honor_ims = True
    compress = True
    status = None               # force this status (with an empty body)
    headers_out = {}
    truncate = False
    requests = []

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        cls = type(self)
        cls.requests.append((self.path, dict(self.headers)))
        if self.headers.get("Authorization") != AUTH:
            return self.reply(401, b"Unauthorized")
        if cls.status:
            return self.reply(cls.status, b"", cls.headers_out)
        ims = self.headers.get("If-Modified-Since")
        if cls.honor_ims and ims and ims == cls.last_modified:
            return self.reply(304, b"")
        body, headers = cls.body, {"Last-Modified": cls.last_modified} if cls.last_modified else {}
        if cls.compress and "gzip" in self.headers.get("Accept-Encoding", ""):
            body, headers["Content-Encoding"] = gzip.compress(body), "gzip"
        self.reply(200, body, headers, extra=100 if cls.truncate else 0)

    def reply(self, code, body, headers=None, extra=0):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body) + extra))
        self.end_headers()
        self.wfile.write(body)
        if extra:
            self.close_connection = True


def fake(**attrs):
    return type("NPRA", (FakeNPRA,), {"requests": [], **attrs})


def serve(handler):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    srv.daemon_threads = True
    Thread(target=srv.serve_forever, daemon=True).start()
    return srv


class DatexCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="datex-test-"))
        self.landing = self.tmp / "landing"
        self.spool = self.tmp / "spool"
        self.spool.mkdir()
        self.servers = []
        self.writers = []

    def tearDown(self):
        for w in self.writers:
            w.close()
        for srv in self.servers:
            srv.shutdown()
            srv.server_close()
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def start(self, handler):
        srv = serve(handler)
        self.servers.append(srv)
        return f"http://127.0.0.1:{srv.server_address[1]}/datexapi"

    def poller(self, handler, feed="situation", auth=AUTH, pause=None):
        writer = hourly.HourlyWriter(self.landing, datex.landing_name(feed))
        self.writers.append(writer)
        return datex.Poller(feed, self.start(handler), auth, "test-client", writer, self.spool,
                            pause or datex.AuthPause())

    def lines(self, feed="situation"):
        """Every stored line, open and closed hours alike: a test that runs across a full hour
        sees the writer rotate, so wait for the closed hour's gzip, then read both kinds."""
        for w in self.writers:
            w.pending.join()
        text = ""
        for p in sorted((self.landing / datex.landing_name(feed)).glob("date=*/hour=*.jsonl*")):
            if p.name.endswith(".gz"):
                text += gzip.decompress(p.read_bytes()).decode("utf-8")
            elif p.name.endswith(".part"):
                text += p.read_text(encoding="utf-8")
        return text, [json.loads(x) for x in text.splitlines()]


class PollTest(DatexCase):
    def test_payload_stored_unchanged(self):
        npra = fake()
        p = self.poller(npra)
        p.poll()
        text, (line,) = self.lines()
        self.assertEqual(list(line), ["requested_at", "received_at", "endpoint", "http_status",
                                      "last_modified", "error", "payload"])
        self.assertEqual(line["payload"], XML)
        self.assertEqual((line["endpoint"], line["http_status"], line["last_modified"], line["error"]),
                         ("situation", 200, LAST_MODIFIED, None))
        self.assertLessEqual(line["requested_at"], line["received_at"])
        # Streamed through the spool, yet byte-identical to dumping the whole line at once.
        self.assertEqual(text, json.dumps(line, ensure_ascii=False) + "\n")
        path, headers = npra.requests[0]
        self.assertEqual(path, "/datexapi/GetSituation/pullsnapshotdata")
        self.assertEqual(headers["User-Agent"], "test-client")
        self.assertEqual(headers["Accept-Encoding"], "gzip")
        self.assertNotIn("If-Modified-Since", headers)
        self.assertEqual(list(self.spool.iterdir()), [])

    def test_polls_across_the_hour(self):
        p = self.poller(fake())
        times = iter([datetime(2026, 10, 24, 18, 59, 59, tzinfo=timezone.utc)] * 2
                     + [datetime(2026, 10, 24, 19, 0, 1, tzinfo=timezone.utc)] * 2)
        with unittest.mock.patch.object(datex, "utcnow", lambda: next(times)):
            p.poll()
            p.poll()
        _, lines = self.lines()
        self.assertEqual([x["received_at"][11:19] for x in lines], ["18:59:59", "19:00:01"])
        day = self.landing / "datex-situation" / "date=2026-10-24"
        self.assertEqual(sorted(f.name for f in day.iterdir()), ["hour=18.jsonl.gz", "hour=19.jsonl.part"])

    def test_uncompressed_response(self):
        p = self.poller(fake(compress=False))
        p.poll()
        self.assertEqual(self.lines()[1][0]["payload"], XML)

    def test_change_detection(self):
        npra = fake(last_modified=None)         # like NPRA today: no Last-Modified
        p = self.poller(npra, feed="weather")
        self.assertEqual(p.poll(), 300)
        first = p.snapshot()["last_change"]
        self.assertEqual(p.poll(), 300)         # same bytes again: stored, but not a change
        state = p.snapshot()
        self.assertEqual(state["polls"], {"changed": 1, "unchanged": 1, "error": 0})
        self.assertEqual(state["last_change"], first)
        self.assertEqual(state["last_bytes"], len(XML.encode("utf-8")))
        npra.body = XML.replace("æøå", "ny").encode("utf-8")
        p.poll()
        self.assertEqual(p.snapshot()["polls"]["changed"], 2)
        _, lines = self.lines("weather")
        self.assertEqual([x["http_status"] for x in lines], [200, 200, 200])
        self.assertTrue(all(x["payload"] for x in lines))
        self.assertNotIn("If-Modified-Since", npra.requests[1][1])

    def test_not_modified_if_last_modified_ever_appears(self):
        npra = fake()
        p = self.poller(npra)
        p.poll()
        self.assertEqual(p.poll(), 300)
        self.assertEqual(npra.requests[1][1]["If-Modified-Since"], LAST_MODIFIED)
        _, lines = self.lines()
        self.assertEqual([x["http_status"] for x in lines], [200, 304])
        self.assertIsNone(lines[1]["payload"])
        self.assertEqual(p.snapshot()["polls"], {"changed": 1, "unchanged": 1, "error": 0})

    def test_server_errors_back_off(self):
        p = self.poller(fake(status=503))
        self.assertEqual([p.poll() for _ in range(4)], [300, 600, 900, 900])
        _, lines = self.lines()
        self.assertTrue(all(x["http_status"] == 503 and x["payload"] is None for x in lines))
        self.assertTrue(lines[0]["error"].startswith("HTTP 503"))
        state = p.snapshot()
        self.assertEqual((state["errors"], state["polls"]["error"], state["last_ok"]), (4, 4, None))
        self.assertEqual(self.poller(fake(status=503), feed="traveltime").poll(), 150)

    def test_retry_after_is_honoured(self):
        p = self.poller(fake(status=429, headers_out={"Retry-After": "700"}))
        self.assertEqual(p.poll(), 700)

    def test_rejected_credentials_pause_every_feed(self):
        pause = datex.AuthPause()
        p = self.poller(fake(), auth="Basic d3Jvbmc6d3Jvbmc=", pause=pause)
        self.assertEqual(p.poll(), datex.AUTH_PAUSE_S)
        self.assertGreater(pause.remaining(), datex.AUTH_PAUSE_S - 5)
        self.assertIn("credentials rejected", pause.reason)
        self.assertEqual(self.lines()[1][0]["http_status"], 401)

    def test_redirect_not_followed(self):
        elsewhere = fake()
        target = self.start(elsewhere)
        p = self.poller(fake(status=302, headers_out={"Location": target + "/GetSituation/pullsnapshotdata"}))
        p.poll()
        line = self.lines()[1][0]
        self.assertEqual(line["http_status"], 302)
        self.assertIn("not followed", line["error"])
        self.assertEqual(elsewhere.requests, [])    # the Authorization header never went there

    def test_truncated_body_is_not_stored(self):
        p = self.poller(fake(compress=False, truncate=True))
        p.poll()
        line = self.lines()[1][0]
        self.assertEqual(line["http_status"], 200)
        self.assertIsNone(line["payload"])
        self.assertIn("IncompleteRead", line["error"])
        self.assertIsNone(p.snapshot()["last_ok"])

    def test_invalid_utf8_is_not_stored(self):
        p = self.poller(fake(body=b"<a>\xff\xfe</a>"))
        p.poll()
        line = self.lines()[1][0]
        self.assertIsNone(line["payload"])
        self.assertIn("UnicodeDecodeError", line["error"])

    def test_unreachable(self):
        writer = hourly.HourlyWriter(self.landing, "datex-situation")
        self.writers.append(writer)
        p = datex.Poller("situation", "http://127.0.0.1:9/datexapi", AUTH, "c", writer, self.spool,
                         datex.AuthPause())
        p.poll()
        line = self.lines()[1][0]
        self.assertIsNone(line["http_status"])
        self.assertIn("URLError", line["error"])


class CheckTest(DatexCase):
    def run_main(self, args, **env):
        out = io.StringIO()
        base = {"DATEX_USERNAME": "user", "DATEX_PASSWORD": "secret", **env}
        with unittest.mock.patch.dict(os.environ, base), contextlib.redirect_stdout(out):
            try:
                code = datex.main(args)
            except SystemExit as e:
                code = e.code
        return code, out.getvalue()

    def test_check(self):
        url = self.start(fake())
        code, out = self.run_main(["--check", "situation", "weather"], DATEX_BASE_URL=url)
        self.assertEqual(code, 0, out)
        self.assertIn("HTTP 200", out)
        self.assertIn("root <d2:payload>", out)
        self.assertEqual(out.count("HTTP 200"), 2)
        self.assertFalse(self.landing.exists())

    def test_check_stops_at_rejected_login(self):
        npra = fake()
        url = self.start(npra)
        code, out = self.run_main(["--check"], DATEX_BASE_URL=url, DATEX_PASSWORD="wrong")
        self.assertEqual(code, 1)
        self.assertIn("credentials rejected", out)
        self.assertEqual(len(npra.requests), 1)

    def test_refuses_plain_http(self):
        code, _ = self.run_main(["--check"], DATEX_BASE_URL="http://datex.example.org/datexapi")
        self.assertIn("must be https", code)


class LifecycleTest(DatexCase):
    def run_service(self, extra, ready, timeout=20):
        state = self.tmp / "run" / "datex.json"
        env = {k: v for k, v in os.environ.items() if not k.startswith("DATEX_")}
        env.update(DAT535_LANDING_DIR=str(self.landing), DATEX_STATE_FILE=str(state),
                   DATEX_SPOOL_DIR=str(self.spool), **extra)
        proc = subprocess.Popen([sys.executable, str(COLLECTOR / "datex.py")], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        seen = None
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    seen = json.loads(state.read_text())
                    if ready(seen):
                        break
                except (FileNotFoundError, ValueError):
                    pass
                time.sleep(0.2)
            proc.send_signal(signal.SIGTERM)
            out, _ = proc.communicate(timeout=30)
        finally:
            if proc.poll() is None:
                proc.kill()
        return proc.returncode, seen, out, state

    def test_polls_and_shuts_down(self):
        url = self.start(fake())
        code, seen, out, state = self.run_service(
            {"DATEX_BASE_URL": url, "DATEX_USERNAME": "user", "DATEX_PASSWORD": "secret",
             "DATEX_FEEDS": "situation"},
            lambda s: s["feeds"]["situation"]["last_ok"])
        self.assertEqual(code, 0, out)
        self.assertIsNone(seen["error"])
        self.assertTrue(seen["polling"])
        self.assertEqual(seen["feeds"]["situation"]["last_status"], 200)
        self.assertFalse(state.exists())        # removed on clean shutdown
        _, lines = self.lines()
        self.assertEqual(lines[0]["payload"], XML)

    def test_without_credentials_runs_but_does_not_poll(self):
        npra = fake()
        code, seen, out, _ = self.run_service({"DATEX_BASE_URL": self.start(npra)},
                                               lambda s: True)
        self.assertEqual(code, 0, out)
        self.assertIn("not set", seen["error"])
        self.assertFalse(seen["polling"])
        self.assertEqual(set(seen["feeds"]), set(datex.FEEDS))
        self.assertEqual(npra.requests, [])
        self.assertFalse(any(self.landing.rglob("*.part")))


if __name__ == "__main__":
    unittest.main()
