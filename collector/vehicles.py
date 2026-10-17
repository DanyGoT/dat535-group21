#!/usr/bin/env python3
"""Entur vehicle positions collector.

Holds one GraphQL subscription to all vehicles in Norway (Entur Vehicle Positions API v2,
`graphql-transport-ws` over a websocket) and appends every message the server sends,
unchanged, as one JSON line to the hourly landing file:

    <landing>/vehicles/date=YYYY-MM-DD/hour=HH.jsonl.part  (gzipped to .jsonl.gz when the hour ends)

Same line format ({"received_at", "endpoint", "payload"}) and hourly files as receiver.py,
whose writer it reuses. Protocol pings are answered, not stored. Reconnects with backoff when
the connection drops, errors or goes silent; every new subscription starts with a snapshot of
all current vehicles, so expect duplicates after a reconnect.

Needs the `websockets` package (Ubuntu: `apt install python3-websockets`, version 10.4).

Environment:
    DAT535_LANDING_DIR      landing root (default: ./landing)
    ET_CLIENT_NAME          ET-Client-Name header (default: uis-dat535group21-collector)
    VEHICLES_URL            default: wss://api.entur.io/realtime/v2/vehicles/subscriptions
"""

import asyncio
import json
import logging
import os
import queue
import signal
import threading
import time
from pathlib import Path

import websockets

from receiver import HourlyWriter, iso, utcnow

log = logging.getLogger("vehicles")

FEED = "vehicles"
SUBPROTOCOL = "graphql-transport-ws"
ACK_TIMEOUT_S = 30
SILENCE_LIMIT_S = 300       # no message for this long = connection presumed dead
MIN_BACKOFF_S = 10
MAX_BACKOFF_S = 300
STABLE_S = 600              # a connection that lasted this long resets the backoff
MAX_MESSAGE = 64 * 1024 * 1024
ROTATE_CHECK_S = 10

# Every field of VehicleUpdate except the epoch-second duplicates of the timestamps.
# Server defaults kept: bufferSize 20, bufferTime 250 ms, includeInvalidLocations false.
QUERY = """subscription {
  vehicles {
    vehicleId
    lastUpdated
    expiration
    codespace { codespaceId }
    operator { operatorRef name }
    line { lineRef lineName publicCode }
    serviceJourney { id date }
    datedServiceJourney { id operatingDay }
    direction
    originRef
    originName
    destinationRef
    destinationName
    mode
    monitored
    location { latitude longitude }
    speed
    bearing
    delay
    inCongestion
    vehicleStatus
    occupancyStatus
    progressBetweenStops { linkDistance percentage }
    monitoredCall { stopPointRef order vehicleAtStop }
  }
}"""


class ProtocolError(Exception):
    """The server broke the graphql-transport-ws protocol or reported an error."""


class Writer(HourlyWriter):
    """HourlyWriter for <root>/<name>/ instead of <root>/siri-<feed>/."""

    def __init__(self, root, name):
        self.dir = Path(root) / name
        self.lock = threading.Lock()
        self.key = None
        self.fh = None
        self.pending = queue.Queue()
        threading.Thread(target=self._compressor, daemon=True).start()
        now = utcnow()
        self.finalize_stale(now)
        self.adopt_current(now)


async def session(url, client_name, writer, silence_s=SILENCE_LIMIT_S):
    """One connection: subscribe, then store messages until the server completes or fails."""
    async with websockets.connect(url, subprotocols=[SUBPROTOCOL],
                                  extra_headers={"ET-Client-Name": client_name},
                                  open_timeout=30, max_size=MAX_MESSAGE) as ws:
        await ws.send(json.dumps({"type": "connection_init", "payload": {}}))
        ack = json.loads(await asyncio.wait_for(ws.recv(), ACK_TIMEOUT_S))
        if ack.get("type") != "connection_ack":
            raise ProtocolError(f"expected connection_ack, got {str(ack)[:200]}")
        await ws.send(json.dumps({"id": "1", "type": "subscribe", "payload": {"query": QUERY}}))
        log.info("subscribed to %s", url)
        while True:
            text = await asyncio.wait_for(ws.recv(), silence_s)
            if isinstance(text, bytes):
                text = text.decode("utf-8")
            kind = json.loads(text).get("type")
            if kind == "ping":
                await ws.send(json.dumps({"type": "pong"}))
                continue
            if kind == "pong":
                continue
            now = utcnow()
            writer.append(json.dumps({"received_at": iso(now), "endpoint": FEED, "payload": text},
                                     ensure_ascii=False) + "\n", now)
            if kind == "error":
                raise ProtocolError(f"subscription error: {text[:500]}")
            if kind == "complete":
                return


async def collect(url, client_name, writer):
    """Keep a session alive forever, with exponential backoff between attempts."""
    backoff = MIN_BACKOFF_S
    while True:
        started = time.monotonic()
        try:
            await session(url, client_name, writer)
            log.warning("server completed the subscription")
        except asyncio.TimeoutError:
            log.error("no message for %d s, reconnecting", SILENCE_LIMIT_S)
        except (OSError, websockets.WebSocketException, ProtocolError, ValueError) as e:
            log.error("connection failed: %s", e)
        if time.monotonic() - started > STABLE_S:
            backoff = MIN_BACKOFF_S
        log.info("reconnecting in %d s", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF_S)


async def rotate(writer):
    while True:
        writer.rotate(utcnow())
        await asyncio.sleep(ROTATE_CHECK_S)


async def main_async(url, client_name, writer):
    tasks = [asyncio.create_task(collect(url, client_name, writer)),
             asyncio.create_task(rotate(writer))]
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: (log.info("signal %s, shutting down", s),
                                                    [t.cancel() for t in tasks]))
    await asyncio.gather(*tasks, return_exceptions=True)


def main():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    landing = Path(os.environ.get("DAT535_LANDING_DIR", "landing"))
    client_name = os.environ.get("ET_CLIENT_NAME", "uis-dat535group21-collector")
    url = os.environ.get("VEHICLES_URL",
                         "wss://api.entur.io/realtime/v2/vehicles/subscriptions")
    writer = Writer(landing, FEED)
    log.info("collecting %s into %s (websockets %s)", url, writer.dir, websockets.__version__)
    try:
        asyncio.run(main_async(url, client_name, writer))
    finally:
        writer.close()


if __name__ == "__main__":
    main()
