"""Tests for the daily NeTEx/NSR archive against a fake download server. No network beyond
localhost; never calls Entur.

Run from the repo root:  python3 -m unittest discover -s collector/tests -v
"""

import io
import json
import os
import shutil
import sys
import tempfile
import unittest
import unittest.mock
import zipfile
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import static  # noqa: E402

NOW = datetime(2026, 10, 27, 4, 15, tzinfo=timezone.utc)


def zip_bytes():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("_shared_data.xml", "<PublicationDelivery>æøå</PublicationDelivery>")
    return buf.getvalue()


class FakeStorage(BaseHTTPRequestHandler):
    body = zip_bytes()
    status = 200
    truncate = False
    requests = []

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        cls = type(self)
        cls.requests.append(self.headers)       # case-insensitive, like HTTP header names
        self.send_response(cls.status)
        self.send_header("Last-Modified", "Mon, 26 Oct 2026 16:29:36 GMT")
        self.send_header("ETag", '"abc"')
        self.send_header("Content-Length", str(len(cls.body) + (100 if cls.truncate else 0)))
        self.end_headers()
        self.wfile.write(cls.body)
        if cls.truncate:
            self.close_connection = True


class ArchiveTest(unittest.TestCase):
    def setUp(self):
        self.landing = Path(tempfile.mkdtemp(prefix="static-test-"))
        self.servers = []

    def tearDown(self):
        for s in self.servers:
            s.shutdown()
            s.server_close()
        shutil.rmtree(self.landing, ignore_errors=True)

    def serve(self, **attrs):
        handler = type("Storage", (FakeStorage,), {"requests": [], **attrs})
        srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        srv.daemon_threads = True
        Thread(target=srv.serve_forever, daemon=True).start()
        self.servers.append(srv)
        return handler, f"http://127.0.0.1:{srv.server_address[1]}/outbound/netex/rb_norway-aggregated-netex.zip"

    def archive(self, url):
        with unittest.mock.patch.dict(os.environ, {"NETEX_URL": url}):
            return static.archive(self.landing, "netex", "test-client", now=NOW)

    def files(self):
        return sorted(str(p.relative_to(self.landing)) for p in self.landing.rglob("*") if p.is_file())

    def test_archives_once_per_day(self):
        server, url = self.serve()
        self.assertEqual(self.archive(url), "archived")
        day = "netex/date=2026-10-27/"
        self.assertEqual(self.files(), [day + "rb_norway-aggregated-netex.zip",
                                        day + "rb_norway-aggregated-netex.zip.meta.json"])
        self.assertEqual((self.landing / day / "rb_norway-aggregated-netex.zip").read_bytes(), FakeStorage.body)
        meta = json.loads((self.landing / day / "rb_norway-aggregated-netex.zip.meta.json").read_text())
        self.assertEqual((meta["last_modified"], meta["etag"], meta["bytes"]),
                         ("Mon, 26 Oct 2026 16:29:36 GMT", '"abc"', len(FakeStorage.body)))
        self.assertEqual(server.requests[0]["ET-Client-Name"], "test-client")
        self.assertEqual(self.archive(url), "present")          # same day: no second download
        self.assertEqual(len(server.requests), 1)

    def test_too_soon_after_the_last_download(self):
        # Yesterday's file was archived late (a manual run): this morning must not download again.
        server, url = self.serve()
        old = self.landing / "netex" / "date=2026-10-26" / "rb_norway-aggregated-netex.zip"
        old.parent.mkdir(parents=True)
        old.write_bytes(FakeStorage.body)
        t = NOW.timestamp() - 5 * 3600
        os.utime(old, (t, t))
        self.assertEqual(self.archive(url), "too-soon")
        self.assertEqual(server.requests, [])
        t = NOW.timestamp() - 25 * 3600
        os.utime(old, (t, t))
        self.assertEqual(self.archive(url), "archived")

    def test_truncated_download_leaves_nothing(self):
        _, url = self.serve(truncate=True)
        with self.assertRaises(ValueError):
            self.archive(url)
        self.assertEqual(self.files(), [])

    def test_not_a_zip_leaves_nothing(self):
        _, url = self.serve(body=b"<html>maintenance</html>")
        with self.assertRaises(zipfile.BadZipFile):
            self.archive(url)
        self.assertEqual(self.files(), [])

    def test_http_error_leaves_nothing(self):
        _, url = self.serve(status=503, body=b"")
        with self.assertRaises(static.urllib.error.HTTPError):
            self.archive(url)
        self.assertEqual(self.files(), [])

    def test_main_exit_code(self):
        _, url = self.serve(status=404, body=b"")
        env = {"DAT535_LANDING_DIR": str(self.landing), "STATIC_DATASETS": "netex", "NETEX_URL": url}
        with unittest.mock.patch.dict(os.environ, env):
            self.assertEqual(static.main(), 1)


if __name__ == "__main__":
    unittest.main()
