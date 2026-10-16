"""Local tests for the Bronze job. Builds a tiny landing zone in a temp dir; never calls Entur.

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

import bronze  # noqa: E402

DAY = date(2026, 10, 15)
ET_XML = '<?xml version="1.0"?>\n<Siri version="2.0"><!-- æøå --></Siri>\n'


def line(feed, payload=ET_XML, received_at="2026-10-15T05:00:01.000Z"):
    # Same shape as the receiver's lines (json.dumps(..., ensure_ascii=False)).
    return json.dumps({"received_at": received_at, "endpoint": feed, "payload": payload},
                      ensure_ascii=False) + "\n"


class LandingCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="bronze-test-"))
        self.landing = self.tmp / "landing"
        self.out = self.tmp / "out"

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def put(self, feed, name, text, day=DAY):
        path = self.landing / f"siri-{feed}" / f"date={day}" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if name.endswith(".gz"):
            with gzip.open(path, "wt", encoding="utf-8") as fh:
                fh.write(text)
        else:
            path.write_text(text, encoding="utf-8")
        return path


class ScanDayTest(LandingCase):
    def test_only_closed_files(self):
        gz = self.put("et", "hour=05.jsonl.gz", line("et"))
        legacy = self.put("et", "hour=04.jsonl", line("et"))
        self.put("et", "hour=06.jsonl.gz.tmp", "")
        self.put("et", "hour=07.jsonl.part", line("et"))
        files, missing, open_hours = bronze.scan_day(self.landing, "et", DAY)
        self.assertEqual(files, [legacy, gz])
        self.assertEqual(open_hours, ["07"])
        self.assertEqual(len(missing), 22)
        self.assertNotIn("04", missing)
        self.assertNotIn("05", missing)

    def test_missing_day(self):
        files, missing, open_hours = bronze.scan_day(self.landing, "sx", DAY)
        self.assertEqual((files, len(missing), open_hours), ([], 24, []))

    def test_two_closed_files_for_one_hour(self):
        self.put("et", "hour=05.jsonl", line("et"))
        self.put("et", "hour=05.jsonl.gz", line("et"))
        with self.assertRaises(RuntimeError):
            bronze.scan_day(self.landing, "et", DAY)


@unittest.skipUnless(find_spec("pyspark"), "pyspark not installed")
class IngestTest(LandingCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master("local[1]").appName("bronze-test") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        super().setUp()
        os.environ.pop("GITHUB_RUN_ID", None)
        self.put("et", "hour=04.jsonl", line("et") + line("et", received_at="2026-10-15T04:59:59.000Z"))
        self.put("et", "hour=05.jsonl.gz",
                 line("et") + "{not json\n" + json.dumps({"received_at": "x"}) + "\n" + line("sx"))
        self.put("et", "hour=06.jsonl.part", line("et"))
        self.put("sx", "hour=05.jsonl.gz", line("sx"))

    def read(self, feed):
        return self.spark.read.parquet(str(self.out / "bronze" / f"siri-{feed}"))

    def test_ingest(self):
        record = bronze.run(self.landing, self.out, DAY, spark=self.spark)
        et = record["feeds"]["et"]
        self.assertEqual(et["files"], 2)
        self.assertEqual(et["rows"], 6)
        self.assertEqual(et["bad_rows"], {"corrupt": 1, "incomplete": 1, "wrong_endpoint": 1})
        self.assertEqual(et["open_hours"], ["06"])
        self.assertEqual(record["feeds"]["sx"]["rows"], 1)
        self.assertTrue((self.out / "bronze" / "_runs" / f"date={DAY}" / f"{record['run_id']}.json").exists())

        rows = self.read("et").collect()
        self.assertEqual(len(rows), 6)
        ok = [r for r in rows if r["_status"] == "ok"]
        self.assertEqual(len(ok), 3)
        self.assertTrue(all(r["payload"] == ET_XML for r in ok))   # payload unchanged
        self.assertEqual(sorted({r["hour"] for r in rows}), ["04", "05"])
        self.assertEqual(len({r["_payload_sha256"] for r in ok}), 1)
        self.assertTrue(all(r["_payload_bytes"] == len(ET_XML.encode("utf-8")) for r in ok))
        corrupt = [r for r in rows if r["_status"] == "corrupt"]
        self.assertEqual(corrupt[0]["_raw_line"], "{not json")
        self.assertTrue(all(r["_source_file"].endswith((".jsonl", ".jsonl.gz")) for r in rows))
        self.assertTrue(all(r["_run_id"] == record["run_id"] for r in rows))

    def test_rerun_replaces_only_that_day(self):
        other = date(2026, 10, 14)
        self.put("et", "hour=23.jsonl.gz", line("et"), day=other)
        bronze.run(self.landing, self.out, other, feeds=["et"], spark=self.spark)
        bronze.run(self.landing, self.out, DAY, feeds=["et"], spark=self.spark)
        second = bronze.run(self.landing, self.out, DAY, feeds=["et"], spark=self.spark)
        df = self.read("et")
        self.assertEqual(df.filter(df.date == str(DAY)).count(), 6)
        self.assertEqual(df.filter(df.date == str(other)).count(), 1)
        self.assertEqual({r["_run_id"] for r in df.filter(df.date == str(DAY)).collect()},
                         {second["run_id"]})

    def test_landing_untouched(self):
        before = sorted((p.relative_to(self.landing), p.stat().st_size, p.stat().st_mtime_ns)
                        for p in self.landing.rglob("*") if p.is_file())
        bronze.run(self.landing, self.out, DAY, spark=self.spark)
        after = sorted((p.relative_to(self.landing), p.stat().st_size, p.stat().st_mtime_ns)
                       for p in self.landing.rglob("*") if p.is_file())
        self.assertEqual(before, after)

    def test_day_without_data_is_skipped(self):
        record = bronze.run(self.landing, self.out, date(2026, 10, 1), spark=self.spark)
        self.assertTrue(record["feeds"]["et"]["skipped"])
        self.assertFalse((self.out / "bronze" / "siri-et").exists())


if __name__ == "__main__":
    unittest.main()
