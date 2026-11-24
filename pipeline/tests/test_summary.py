"""Tests for pipeline/summary.py: step summaries and multi-day Silver quality reports."""

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import summary  # noqa: E402

EVIDENCE = Path(__file__).resolve().parents[2] / "docs" / "report" / "evidence"


def sample_bronze_entur():
    return {
        "run_id": "gh-100-1",
        "date": "2026-11-20",
        "started_at": "2026-11-21T03:00:00Z",
        "finished_at": "2026-11-21T03:11:30Z",
        "feeds": {
            "et": {"files": 24, "rows": 4800000, "bad_rows": {}},
            "sx": {"files": 24, "rows": 4500, "bad_rows": {}},
            "vehicles": {"files": 24, "rows": 620000, "bad_rows": {"corrupt": 1}},
        },
    }


def sample_bronze_datex():
    return {
        "run_id": "gh-100-1",
        "source": "datex",
        "date": "2026-11-20",
        "started_at": "2026-11-21T03:12:00Z",
        "finished_at": "2026-11-21T03:12:55Z",
        "feeds": {
            "situation": {
                "files": 24,
                "rows": 288,
                "bad_rows": {},
                "polls": {"ok": 286, "not_modified": 0, "fetch_error": 2},
            },
        },
    }


def sample_silver_entur(day="2026-11-19", started="2026-11-21T03:20:00Z"):
    return {
        "run_id": f"gh-100-{day}",
        "schema_version": 2,
        "operating_date": day,
        "started_at": started,
        "status": "complete",
        "ready": True,
        "elapsed_s": 1820.5,
        "inputs": {
            "et": {
                "2026-11-18": {"rows": 1000},
                "2026-11-19": {"rows": 2000},
                "2026-11-20": {"rows": 1500},
                "decoded": {"call": 100000, "journey": 5000, "control": 100},
                "retained_versions": {"call": 95000, "journey": 4800},
            },
            "sx": {"decoded": {"sx": 200, "control": 10}},
            "vehicles": {"decoded": {"vehicle": 10000}},
        },
        "tables": {
            "journey": {"rows": 1000, "flagged_rows": 5, "issues": {"unknown_line": 5}},
            "stop_call": {"rows": 20000, "flagged_rows": 50, "issues": {"unknown_quay": 50}},
            "call_prediction": {"rows": 200000, "flagged_rows": 0, "issues": {}},
            "vehicle_position": {"rows": 9900, "flagged_rows": 10, "issues": {"invalid_bearing": 10}},
            "sx_situation": {"rows": 190, "flagged_rows": 0, "issues": {}},
            "quarantine": {"rows": 1152, "flagged_rows": 1152, "issues": {"missing_operating_date": 1152}},
        },
        "call_coverage": {"matched": 19000, "no_realtime": 500, "realtime_only": 500},
        "call_reconciliation": {
            "input_versions": 95000,
            "planned_calls": 19500,
            "reported_calls": 19500,
            "duplicate_versions_removed": 75500,
        },
        "actual_equals_aimed_by_operator": [
            {"operator_id": "RUT:Operator:210", "actual_departures": 1000, "actual_equals_aimed": 250},
            {"operator_id": "TINY:Op:1", "actual_departures": 50, "actual_equals_aimed": 50},
        ],
        "contract_errors": {},
    }


def sample_silver_datex(day="2026-11-20"):
    return {
        "run_id": "gh-100-1",
        "source": "datex",
        "schema_version": 1,
        "observation_date": day,
        "started_at": "2026-11-21T03:13:00Z",
        "status": "complete",
        "ready": True,
        "elapsed_s": 110.2,
        "inputs": {
            "situation": {"rows": 288},
            "weather": {"rows": 288},
        },
        "tables": {
            "road_situation": {"rows": 3100, "flagged_rows": 12, "issues": {"missing_location": 12}},
            "road_situation_interval": {"rows": 3500, "flagged_rows": 0, "issues": {}},
            "road_weather": {"rows": 45000, "flagged_rows": 200, "issues": {"stale_measurement": 200}},
            "road_travel_time": {"rows": 58000, "flagged_rows": 0, "issues": {}},
            "weather_site": {"rows": 475, "flagged_rows": 19, "issues": {"no_current_measurement": 19}},
            "road_section": {"rows": 202, "flagged_rows": 0, "issues": {}},
            "datex_quarantine": {"rows": 1, "flagged_rows": 1, "issues": {"source_delivery_failed": 1}},
        },
        "contract_errors": {},
    }


def sample_gold(day="2026-11-19"):
    return {
        "run_id": "gh-100-1",
        "operating_date": day,
        "schema_version": 2,
        "status": "complete",
        "ready": True,
        "elapsed_s": 64.3,
        "tables": {"line_scorecard": {"rows": 3900}},
        "totals": {
            "input_calls": 20000,
            "scored_departures": 16000,
            "late_departures": 4800,
        },
    }


class SummaryTests(unittest.TestCase):
    def test_classify_all_layers(self):
        self.assertEqual(summary.classify(sample_bronze_entur()), "bronze_entur")
        self.assertEqual(summary.classify(sample_bronze_datex()), "bronze_datex")
        self.assertEqual(summary.classify(sample_silver_entur()), "silver_entur")
        self.assertEqual(summary.classify(sample_silver_datex()), "silver_datex")
        self.assertEqual(summary.classify(sample_gold()), "gold")
        self.assertEqual(summary.classify({"hello": "world"}), "unknown")

    def test_step_summary_renders_all_layers_in_order(self):
        md = summary.render_step_summary([
            sample_gold(),
            sample_silver_entur(),
            sample_bronze_datex(),
            sample_bronze_entur(),
            sample_silver_datex(),
        ])
        self.assertIn("| Bronze Entur | 2026-11-20 | ok (1 bad) |", md)
        self.assertIn("| Bronze DATEX | 2026-11-20 | ok |", md)
        self.assertIn("| Silver DATEX | 2026-11-20 | complete (ready) |", md)
        self.assertIn("| Silver Entur | 2026-11-19 | complete (ready) |", md)
        self.assertIn("| Gold | 2026-11-19 | complete (ready) |", md)
        self.assertIn("4,800 late (30.00%)", md)
        self.assertLess(md.index("Bronze Entur"), md.index("Silver Entur"))
        self.assertLess(md.index("Silver Entur"), md.index("Gold"))

    def test_step_summary_handles_empty_and_failed_runs(self):
        self.assertIn("No run record written.", summary.render_step_summary([]))
        failed = sample_silver_entur()
        failed["status"] = "failed"
        failed["ready"] = False
        failed["error"] = "ValueError: missing Bronze"
        skipped = sample_gold()
        skipped["skipped_tables"] = ["departure_conditions"]
        md = summary.render_step_summary([failed, skipped])
        self.assertIn("ValueError: missing Bronze", md)
        self.assertIn("complete (ready, skipped: departure_conditions)", md)

    def test_quality_report_funnel_quarantine_rate_and_dedup(self):
        older = sample_silver_entur("2026-11-19", started="2026-11-20T03:00:00Z")
        older["tables"]["stop_call"]["rows"] = 999999
        newer = sample_silver_entur("2026-11-19", started="2026-11-21T03:20:00Z")
        md = summary.render_quality_report([older, newer, sample_silver_datex()])
        # Newest run wins for 2026-11-19.
        self.assertIn("| 2026-11-19 | 2 | complete (ready) | 4,500 | 100,000 | 95,000 | 20,000 | 75,500 |", md)
        self.assertNotIn("999,999", md)
        # Quarantine rate is measured against decoded non-control versions (100000+5000+200+10000 = 115200 -> 1.00%).
        self.assertIn("**1,152** of **115,200** decoded non-control versions (**1.00%**)", md)
        # Operator threshold filters out TINY:Op:1 (< 100 departures).
        self.assertIn("RUT:Operator:210", md)
        self.assertIn("25.00%", md)
        self.assertNotIn("TINY:Op:1", md)
        # DATEX section included.
        self.assertIn("stale_measurement", md)
        self.assertIn("source_delivery_failed", md)

    def test_cli_reads_real_archived_evidence_and_skips_latest_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "latest.json").write_text(json.dumps(sample_silver_entur("2026-11-18")))
            (root / "run.json").write_text(json.dumps(sample_silver_entur("2026-11-19")))
            (root / "noise.json").write_text('{"not_a_run": true}')
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = summary.main(["quality", str(root)])
            self.assertEqual(rc, 0)
            self.assertIn("2026-11-19", buf.getvalue())
            self.assertNotIn("2026-11-18", buf.getvalue())

        # Also smoke-test against the real archived evidence JSONs in docs/report/evidence/.
        real = [EVIDENCE / "silver-2026-10-29.json", EVIDENCE / "gold-2026-11-09.json"]
        if all(p.exists() for p in real):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = summary.main(["summary", *[str(p) for p in real]])
            self.assertEqual(rc, 0)
            self.assertIn("1,869,699 calls", buf.getvalue())
            self.assertIn("1,627,100 timed", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
