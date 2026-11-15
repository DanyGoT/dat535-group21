"""What the pipeline tests share: the pipeline modules on sys.path and one local Spark session.

Run from the repo root (Spark tests need pyspark and Java, e.g. inside ~/spark-env):
    python3 -m unittest discover -s pipeline/tests -v
"""

import gzip
import os
import sys
import tempfile
import unittest
from importlib.util import find_spec
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@unittest.skipUnless(find_spec("pyspark"), "pyspark not installed")
class SparkCase(unittest.TestCase):
    """A test class with self.spark. Every class gets the same session, so the JVM starts once per
    test run. UTC like the jobs; two cores and two shuffle partitions keep tiny fixtures fast."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master("local[2]").appName("dat535-tests") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.parquet.compression.codec", "zstd") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")


class LandingCase(unittest.TestCase):
    """A temporary landing zone and output directory per test. Runs in it get local run ids, even
    in CI. Subclasses set `day`, and `dataset`: a feed's landing directory."""
    day = None

    @staticmethod
    def dataset(feed):
        return feed

    def setUp(self):
        self.tmp = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="dat535-test-")))
        self.landing = self.tmp / "landing"
        self.out = self.tmp / "out"
        self.enterContext(patch.dict(os.environ))
        os.environ.pop("GITHUB_RUN_ID", None)

    def put(self, feed, name, text, day=None):
        """Write one landing file, gzipped when its name ends in .gz."""
        path = self.landing / self.dataset(feed) / f"date={day or self.day}" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if name.endswith(".gz"):
            with gzip.open(path, "wt", encoding="utf-8") as fh:
                fh.write(text)
        else:
            path.write_text(text, encoding="utf-8")
        return path

    def landing_files(self):
        """Every landing file with its size and modification time: equal before and after a run
        that left landing alone."""
        return sorted((p.relative_to(self.landing), p.stat().st_size, p.stat().st_mtime_ns)
                      for p in self.landing.rglob("*") if p.is_file())
