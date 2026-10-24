"""Local tests for the DATEX Bronze job. Builds a tiny landing zone in a temp dir; never calls
Vegvesen.

Run from the repo root (Spark tests need pyspark + Java, e.g. inside ~/spark-env):
    python3 -m unittest discover -s pipeline/tests -v
"""

import gzip
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import date
from importlib.util import find_spec
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import bronze_datex  # noqa: E402

DAY = date(2026, 10, 24)
XML = ('<?xml version=\'1.0\' encoding=\'UTF-8\' standalone=\'yes\'?>\n<ns2:messageContainer '
       'xmlns:ns2="http://datex2.eu/schema/3/messageContainer"><!-- Bergen–Åsane æøå --></ns2:messageContainer>')


def poll(feed="situation", at="05:00:01", status=200, payload=XML, error=None, **extra):
    # Same shape and key order as datex.py's lines (json.dumps(..., ensure_ascii=False)).
    rec = {"requested_at": f"{DAY}T{at}.000Z", "received_at": f"{DAY}T{at}.500Z", "endpoint": feed,
           "http_status": status, "last_modified": None, "error": error,
           "payload": payload if status == 200 and error is None else None}
    rec.update(extra)
    return json.dumps(rec, ensure_ascii=False) + "\n"


@unittest.skipUnless(find_spec("pyspark"), "pyspark not installed")
class IngestTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master("local[1]").appName("bronze-datex-test") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="bronze-datex-test-"))
        self.landing = self.tmp / "landing"
        self.out = self.tmp / "out"
        os.environ.pop("GITHUB_RUN_ID", None)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def put(self, feed, name, text, day=DAY):
        path = self.landing / f"datex-{feed}" / f"date={day}" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if name.endswith(".gz"):
            with gzip.open(path, "wt", encoding="utf-8") as fh:
                fh.write(text)
        else:
            path.write_text(text, encoding="utf-8")
        return path

    def read(self, feed):
        return self.spark.read.parquet(str(self.out / "bronze" / f"datex-{feed}"))

    def test_ingest(self):
        self.put("situation", "hour=05.jsonl.gz",
                 poll(at="05:00:01") + poll(at="05:05:01")
                 + poll(at="05:10:01", payload=XML.replace("æøå", "ny"))
                 + poll(at="05:15:01", status=503, error="HTTP 503 Service Unavailable")
                 + poll(at="05:20:01", status=None, error="URLError: timed out"))
        self.put("situation", "hour=06.jsonl.gz",
                 poll(at="06:00:01", status=304)
                 + "{not json\n"
                 + json.dumps({"received_at": f"{DAY}T06:01:00.000Z", "endpoint": "situation"}) + "\n"
                 + poll(feed="weather", at="06:02:00")
                 + poll(at="06:03:00", status=200, payload=None))      # 200 without payload
        self.put("situation", "hour=07.jsonl.part", poll(at="07:00:01"))
        self.put("weather", "hour=05.jsonl.gz", poll(feed="weather"))

        record = bronze_datex.run(self.landing, self.out, DAY, spark=self.spark)
        self.assertEqual(record["source"], "datex")
        s = record["feeds"]["situation"]
        self.assertEqual((s["files"], s["rows"], s["open_hours"]), (2, 10, ["07"]))
        self.assertEqual(s["polls"], {"ok": 3, "not_modified": 1, "fetch_error": 2})
        self.assertEqual(s["bad_rows"], {"corrupt": 1, "incomplete": 2, "wrong_endpoint": 1})
        self.assertEqual(s["snapshots"], 2)
        self.assertEqual((s["first_ok_at"], s["last_ok_at"]), (f"{DAY}T05:00:01.500Z", f"{DAY}T06:00:01.500Z"))
        self.assertEqual(s["max_gap_s"], 50 * 60)       # 05:10 -> 06:00, the failed polls between
        self.assertEqual(s["parquet_files"], 1)          # small input: one file, not one per hour
        self.assertEqual(record["feeds"]["weather"]["rows"], 1)
        for skipped in ("traveltime", "traveltime-locations", "weather-sites"):
            self.assertTrue(record["feeds"][skipped]["skipped"])
        self.assertTrue((self.out / "bronze" / "_runs" / f"date={DAY}" / f"{record['run_id']}.datex.json").exists())

        rows = {r["received_at"]: r for r in self.read("situation").collect()}
        first = rows[f"{DAY}T05:00:01.500Z"]
        self.assertEqual(first["payload"], XML)          # payload unchanged
        self.assertEqual((first["_bronze_status"], first["http_status"], first["hour"]), ("ok", 200, "05"))
        self.assertEqual(first["_bronze_payload_bytes"], len(XML.encode("utf-8")))
        self.assertEqual(first["_bronze_payload_sha256"], rows[f"{DAY}T05:05:01.500Z"]["_bronze_payload_sha256"])
        timeout = rows[f"{DAY}T05:20:01.500Z"]
        self.assertEqual((timeout["_bronze_status"], timeout["http_status"], timeout["error"]),
                         ("fetch_error", None, "URLError: timed out"))
        self.assertEqual(rows[f"{DAY}T06:00:01.500Z"]["_bronze_status"], "not_modified")
        self.assertEqual(rows[f"{DAY}T06:03:00.500Z"]["_bronze_status"], "incomplete")

    def test_snapshot_longer_than_jacksons_default(self):
        big = "<a>" + "x" * 20_000_100 + "</a>"         # a real situation snapshot is ~28.6M chars
        self.put("situation", "hour=05.jsonl.gz", poll(payload=big))
        record = bronze_datex.run(self.landing, self.out, DAY, feeds=["situation"], spark=self.spark)
        self.assertEqual(record["feeds"]["situation"]["polls"]["ok"], 1)
        self.assertEqual(record["feeds"]["situation"]["bad_rows"], {})

    def test_rerun_and_hours(self):
        self.put("weather", "hour=05.jsonl.gz", poll(feed="weather"))
        self.put("weather", "hour=08.jsonl.gz", poll(feed="weather", at="08:00:00"))
        bronze_datex.run(self.landing, self.out, DAY, feeds=["weather"], spark=self.spark)
        second = bronze_datex.run(self.landing, self.out, DAY, feeds=["weather"], spark=self.spark,
                                  hours=["08"])
        df = self.read("weather")
        self.assertEqual([r["hour"] for r in df.collect()], ["08"])
        self.assertEqual(second["hours"], ["08"])
        self.assertEqual({r["_bronze_run_id"] for r in df.collect()}, {second["run_id"]})

    def test_landing_untouched(self):
        self.put("situation", "hour=05.jsonl.gz", poll())
        self.put("situation", "hour=06.jsonl.part", poll(at="06:00:00"))
        before = sorted((p.relative_to(self.landing), p.stat().st_size, p.stat().st_mtime_ns)
                        for p in self.landing.rglob("*") if p.is_file())
        bronze_datex.run(self.landing, self.out, DAY, spark=self.spark)
        after = sorted((p.relative_to(self.landing), p.stat().st_size, p.stat().st_mtime_ns)
                       for p in self.landing.rglob("*") if p.is_file())
        self.assertEqual(before, after)


class CliTest(unittest.TestCase):
    def test_unknown_feed(self):
        with self.assertRaises(SystemExit):
            bronze_datex.main(["--feeds", "et"])


if __name__ == "__main__":
    unittest.main()
