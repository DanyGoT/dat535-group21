"""Tests for peek.py: shows data, skips heartbeats and half-written lines. Local files only.

Run from the repo root:  python3 -m unittest discover -s collector/tests -v
"""

import gzip
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
COLLECTOR = HERE.parent

HEARTBEAT = ('<?xml version="1.0" encoding="UTF-8"?><Siri xmlns="http://www.siri.org.uk/siri" '
             'version="2.0"><HeartbeatNotification><Status>true</Status></HeartbeatNotification></Siri>')
ET_DATA = ('<?xml version="1.0" encoding="UTF-8"?><Siri xmlns="http://www.siri.org.uk/siri" version="2.0">'
           '<ServiceDelivery><EstimatedTimetableDelivery><EstimatedJourneyVersionFrame>'
           '<EstimatedVehicleJourney><LineRef>SKY:Line:760</LineRef><RecordedCalls><RecordedCall>'
           '<StopPointRef>NSR:Quay:1</StopPointRef></RecordedCall></RecordedCalls></EstimatedVehicleJourney>'
           '</EstimatedJourneyVersionFrame></EstimatedTimetableDelivery></ServiceDelivery></Siri>')
VEHICLES = json.dumps({"id": "1", "type": "next", "payload": {"data": {"vehicles": [
    {"vehicleId": "a", "line": {"lineName": "Bergen–Åsane"}}, {"vehicleId": "b"}]}}})


def line(endpoint, payload):
    return json.dumps({"received_at": "2026-10-17T13:00:00.000Z", "endpoint": endpoint,
                       "payload": payload}, ensure_ascii=False) + "\n"


class PeekTest(unittest.TestCase):
    def peek(self, landing, *args):
        r = subprocess.run([sys.executable, str(COLLECTOR / "peek.py"), *args], capture_output=True,
                           text=True, timeout=30, env=dict(os.environ, DAT535_LANDING_DIR=str(landing)))
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout

    def test_et_part_skips_heartbeats_and_partial_line(self):
        with tempfile.TemporaryDirectory() as d:
            part = Path(d) / "siri-et" / "date=2026-10-17" / "hour=13.jsonl.part"
            part.parent.mkdir(parents=True)
            part.write_text(line("et", HEARTBEAT) * 50 + line("et", ET_DATA) + line("et", HEARTBEAT) * 50
                            + line("et", ET_DATA)[:40], encoding="utf-8")
            out = self.peek(d, "et", "-n", "1")
        self.assertIn("<EstimatedVehicleJourney>\n  <LineRef>SKY:Line:760</LineRef>", out)
        self.assertIn("EstimatedVehicleJourney 1 of 1 in this push", out)
        self.assertNotIn("Heartbeat", out)
        self.assertNotIn("xmlns", out)

    def test_library(self):
        sys.path.insert(0, str(COLLECTOR))
        import peek
        with tempfile.TemporaryDirectory() as d:
            part = Path(d) / "siri-et" / "date=2026-10-17" / "hour=13.jsonl.part"
            part.parent.mkdir(parents=True)
            part.write_text(line("et", HEARTBEAT) + line("et", ET_DATA), encoding="utf-8")
            msgs = peek.sample("et", 1, landing=d)
        self.assertEqual(len(msgs), 1)
        journey = msgs[0]["items"][0]
        self.assertEqual(peek.flatten(journey)["LineRef"], "SKY:Line:760")
        self.assertEqual(peek.calls(journey), [{"kind": "RecordedCall", "StopPointRef": "NSR:Quay:1"}])
        self.assertNotIn("\n", peek.flatten(journey).get("LineRef"))

    def test_vehicles_closed_file(self):
        with tempfile.TemporaryDirectory() as d:
            gz = Path(d) / "hour=12.jsonl.gz"
            gz.write_bytes(gzip.compress((line("vehicles", '{"type":"ping"}')
                                          + line("vehicles", VEHICLES)).encode("utf-8")))
            out = self.peek(d, "vehicles", "--file", str(gz))
        self.assertIn("2 of 2 vehicles in this message", out)
        self.assertIn('"lineName": "Bergen–Åsane"', out)


if __name__ == "__main__":
    unittest.main()
