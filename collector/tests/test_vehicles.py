"""Local tests for the vehicle positions collector against a fake graphql-transport-ws server.
No network beyond localhost; never calls Entur. Skipped where `websockets` is not installed.

Run from the repo root:  python3 -m unittest discover -s collector/tests -v
"""

import asyncio
import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

try:
    import websockets
    import vehicles
except ImportError:  # pragma: no cover
    websockets = None

NEXT_1 = '{"id":"1","type":"next","payload":{"data":{"vehicles":[{"vehicleId":"a","line":{"lineName":"Bergen–Åsane"}}]}}}'
NEXT_2 = '{"id":"1","type":"next","payload":{"data":{"vehicles":[{"vehicleId":"b"}]}}}'


@unittest.skipIf(websockets is None, "websockets not installed")
class SessionTest(unittest.TestCase):
    def run_session(self, script):
        """Serve one connection that plays `script`; return (lines stored, what the server saw)."""
        seen = {}

        async def handler(ws):
            seen["header"] = ws.request_headers.get("ET-Client-Name")
            seen["subprotocol"] = ws.subprotocol
            seen["init"] = json.loads(await ws.recv())
            await ws.send('{"type":"connection_ack"}')
            seen["subscribe"] = json.loads(await ws.recv())
            await script(ws, seen)

        async def go(d):
            writer = vehicles.HourlyWriter(d, vehicles.FEED)
            async with websockets.serve(handler, "127.0.0.1", 0,
                                        subprotocols=[vehicles.SUBPROTOCOL]) as srv:
                port = srv.sockets[0].getsockname()[1]
                try:
                    await vehicles.session(f"ws://127.0.0.1:{port}", "test-client", writer,
                                           silence_s=5)
                    seen["result"] = "complete"
                except vehicles.ProtocolError as e:
                    seen["result"] = e
            writer.close()

        with tempfile.TemporaryDirectory() as d:
            asyncio.run(go(d))
            lines = []
            for part in sorted(Path(d, "vehicles").glob("date=*/hour=*.jsonl.part")):
                lines += part.read_text(encoding="utf-8").splitlines()
        return [json.loads(x) for x in lines], seen

    def test_stores_messages_unchanged_and_answers_ping(self):
        async def script(ws, seen):
            await ws.send(NEXT_1)
            await ws.send('{"type":"ping"}')
            seen["pong"] = json.loads(await ws.recv())
            await ws.send(NEXT_2.encode("utf-8"))      # binary frame
            await ws.send('{"id":"1","type":"complete"}')
            await asyncio.sleep(0.2)

        lines, seen = self.run_session(script)
        self.assertEqual(seen["header"], "test-client")
        self.assertEqual(seen["subprotocol"], "graphql-transport-ws")
        self.assertEqual(seen["init"]["type"], "connection_init")
        self.assertEqual(seen["subscribe"]["type"], "subscribe")
        self.assertIn("vehicles", seen["subscribe"]["payload"]["query"])
        self.assertEqual(seen["pong"], {"type": "pong"})
        self.assertEqual(seen["result"], "complete")
        self.assertEqual([x["payload"] for x in lines],
                         [NEXT_1, NEXT_2, '{"id":"1","type":"complete"}'])
        self.assertEqual({x["endpoint"] for x in lines}, {"vehicles"})
        self.assertTrue(all(x["received_at"].endswith("Z") for x in lines))

    def test_error_is_stored_and_raised(self):
        error = '{"id":"1","type":"error","payload":[{"message":"bad query"}]}'

        async def script(ws, seen):
            await ws.send(error)
            await asyncio.sleep(0.2)

        lines, seen = self.run_session(script)
        self.assertIsInstance(seen["result"], vehicles.ProtocolError)
        self.assertEqual([x["payload"] for x in lines], [error])

    def test_closed_hour_is_gzipped(self):
        with tempfile.TemporaryDirectory() as d:
            w = vehicles.HourlyWriter(d, vehicles.FEED)
            from datetime import datetime, timezone
            w.append("a\n", datetime(2026, 10, 15, 10, 59, tzinfo=timezone.utc))
            w.rotate(datetime(2026, 10, 15, 11, 0, tzinfo=timezone.utc))
            w.pending.join()
            gz = Path(d) / "vehicles" / "date=2026-10-15" / "hour=10.jsonl.gz"
            self.assertEqual(gzip.decompress(gz.read_bytes()), b"a\n")


if __name__ == "__main__":
    unittest.main()
