"""Read-only CPU microprofile of the existing ET decoder and PySpark row conversion.

Imports Spark's Python types but creates no Spark context or JVM. Run separately
with a small CPU/memory limit; this does not modify the live validation or source.
"""

import argparse
import cProfile
import io
import json
import platform
import pstats
import time
from pathlib import Path

import findspark
findspark.init("/opt/spark")
import pyspark
from pyspark.sql.types import _make_type_verifier

import silver_parse as P


def profile(source, destination, repeats=200):
    rows = []
    for line in Path(source).read_text().splitlines():
        envelope = json.loads(line)
        sha = P.digest(envelope["payload"])
        rows.append(dict(envelope, _bronze_source_file=str(source), _bronze_payload_sha256=sha,
                         record_sha256=sha, _bronze_run_id="profile", _bronze_status="ok",
                         source_date=envelope["received_at"][:10]))
    schema = P.schema("et")
    verify = _make_type_verifier(schema)
    decoded = list(P.decode_partition(iter(rows), "et"))

    def run_case(decode, validate, convert, iterations):
        count = 0
        for _ in range(iterations):
            values = P.decode_partition(iter(rows), "et") if decode else iter(decoded)
            for value in values:
                if validate:
                    verify(value)
                if convert:
                    schema.toInternal(value)
                count += 1
        return count

    results = []
    for name, decode, validate, convert in (
        ("decode_only", True, False, False),
        ("decode_and_convert", True, False, True),
        ("decode_verify_convert", True, True, True),
        ("verify_predecoded", False, True, False),
        ("convert_predecoded", False, False, True),
    ):
        started = time.process_time()
        count = run_case(decode, validate, convert, repeats)
        elapsed = time.process_time() - started
        if count != len(decoded) * repeats:
            raise ValueError("Microprofile record counts changed")
        results.append(dict(case=name, rows=count, cpu_s=elapsed, rows_per_cpu_s=count / elapsed))
    profiler = cProfile.Profile()
    profiler.runcall(run_case, True, True, True, min(repeats, 40))
    stream = io.StringIO()
    pstats.Stats(profiler, stream=stream).strip_dirs().sort_stats("cumulative").print_stats(25)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    result = dict(python=platform.python_version(), spark=pyspark.__version__,
                  source_messages=len(rows), decoded_versions=len(decoded), repeats=repeats,
                  cases=results, note="Small real-data CPU microprofile; not a full-day performance benchmark")
    (destination / "decode-benchmark.json").write_text(json.dumps(result, indent=2) + "\n")
    (destination / "decode-profile.txt").write_text(stream.getvalue())
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    profile(args.source, args.destination)
