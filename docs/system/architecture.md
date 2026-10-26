# Architecture

Neither source keeps the raw data we need: Entur has no archive of raw real-time messages
(only processed BigQuery tables, [entur/sources.md](../entur/sources.md)), and NPRA's DATEX only serves the current
snapshot ([datex/sources.md](../datex/sources.md)). So always-on collectors store everything as it arrives, and scheduled
Spark jobs turn it into Medallion layers. Everything runs on one UiS VM ([system/storage.md](storage.md)).

```
Entur SIRI ET/SX ──push──────▶ receiver.py ──┐
Entur vehicles ───websocket──▶ vehicles.py ──┼──▶ landing/<dataset>/date=D/hour=HH.jsonl.gz
NPRA DATEX ◀─────poll (GET)── datex.py ──────┘     raw, hourly, UTC, never modified
Entur NeTEx + NSR ── daily download (not built yet) ──▶ landing/netex, landing/nsr
                                                   │
  monitor.py   (reads) ◀───────────────────────────┤──▶ status page + ntfy on OK↔FAIL
  exporter.py  (reads) ◀───────────────────────────┤──▶ Prometheus ──▶ Grafana (/grafana/)
                                                   ▼
  Spark (GitHub Actions, 03:00 UTC): Bronze ──▶ Silver ──▶ Gold      /data/dat535/{prod,dev}
```

## Components

| Component | Code | Runs as | State (2026-10-24) |
| --------- | ---- | ------- | ------------------ |
| SIRI receiver | `collector/receiver.py` | `dat535-receiver.service` | running since 2026-10-15 |
| Vehicle positions | `collector/vehicles.py` | `dat535-vehicles.service` | running since 2026-10-17 |
| DATEX poller | `collector/datex.py` | `dat535-datex.service` | ready; credentials on the VM since 2026-10-26, starts with the next deploy |
| NeTEx + NSR download | — | — | **not built**: needed by most Gold products ([pipeline/gold.md](../pipeline/gold.md)) |
| Monitor + status page | `collector/monitor.py`, `serve_status.py` | timer (2 min), `dat535-status` | running |
| Metrics | `collector/exporter.py` + Prometheus + Grafana | `dat535-exporter`, `-prometheus`, `-grafana` | running |
| Tunnel | `systemd/dat535-tunnel.service` | reverse SSH to a public host | running |
| Bronze | `pipeline/bronze.py` (SIRI), `pipeline/bronze_datex.py` | workflow step | running (SIRI), new (DATEX) |
| Silver, Gold | — | — | not built ([pipeline/silver.md](../pipeline/silver.md), [pipeline/gold.md](../pipeline/gold.md)) |

## Collectors

All three follow the same rules:

- **Dumb.** No parsing, no filtering. Every message (or poll) becomes one JSON line, with the
  payload unchanged inside it.
- **One writer.** `collector/hourly.py` appends to `hour=HH.jsonl.part` and, when the hour
  ends, gzips it to `hour=HH.jsonl.gz` (via `.gz.tmp` and an atomic rename). Closed files are
  never touched again; a restart adopts the open `.part` and closes stale ones.
- **Coverage evidence is data too.** SIRI heartbeats, failed DATEX polls and every repeated
  snapshot are stored, so gaps can be measured later.

**SIRI receiver.** Holds SIRI 2.0 pub/sub subscriptions (ET, SX) with Entur, which POSTs to
our public address (via the tunnel). Re-subscribes on silence (3 missed heartbeats) and every
24 h. Protocol details: [entur/realtime.md](../entur/realtime.md).

**Vehicle positions.** One GraphQL subscription over a websocket to all vehicles in Norway;
reconnects with backoff; every reconnect starts with a full snapshot. Ruter publishes no
positions there.

**DATEX poller.** One thread per feed polls NPRA's pull services with Basic Auth; one line per
poll, failures included. Without credentials it runs idle (not a failure). Feeds, intervals
and sizes: [datex/sources.md](../datex/sources.md).

## Landing zone

The contract between the collectors and Spark.

```
/data/dat535/landing/
├── siri-et/date=YYYY-MM-DD/hour=HH.jsonl.gz       (hour=HH.jsonl.part while open)
├── siri-sx/...
├── vehicles/...
├── datex-<feed>/...   situation, traveltime, weather, traveltime-locations, weather-sites
├── netex/date=YYYY-MM-DD/rb_norway-aggregated-netex.zip    (planned)
└── nsr/date=YYYY-MM-DD/Current_latest.zip                  (planned)
```

| Source | Line |
| ------ | ---- |
| SIRI, vehicles | `{"received_at": "<UTC>", "endpoint": "et\|sx\|vehicles", "payload": "<raw XML or JSON message>"}` |
| DATEX | `{"requested_at", "received_at", "endpoint": "<feed>", "http_status", "last_modified", "error", "payload": "<XML or null>"}` |

- `date=` and `hour=` are **UTC**, from `received_at`: no DST duplicates or gaps. Operating
  days are Norwegian local time; Silver maps between them.
- **Readers take closed files only** (`*.jsonl.gz`, and plain `*.jsonl` for hours closed before
  2026-10-15); never `*.part` or `*.tmp`.
- **JSON Lines, not concatenated XML:** each message stays one valid record, and a day can be
  split by hour.
- **Hourly files:** few enough for Spark to list, small enough to rerun one day or read while
  the collectors write. Gzip is not splittable; one task per hour is fine at this size.
- Payloads can exceed 20M characters: every Spark JSON read needs `maxStringLen` ([system/storage.md](storage.md)).
- **Immutable.** Only the collectors write here, and nothing modifies or deletes files
  automatically. When the disk fills, a person deletes whole old days, cheapest loss first
  ([system/storage.md](storage.md)).

## Batch pipeline

- The prod workflow runs at 03:00 UTC for the previous UTC day (after hour 23 has closed);
  manual reruns take a date. The dev workflow runs one hour of yesterday on every push and PR
  to `dev`. Outputs: `/data/dat535/prod` and `/data/dat535/dev`, next to the landing zone.
- **Bronze**: closed hourly files → zstd Parquet per dataset and day, payload unchanged, plus
  evidence columns (`_bronze_*`: source file, status, payload hash and size, run id) and a run
  record per run under `bronze/_runs/`. `bronze.py` reads SIRI; `bronze_datex.py` reads DATEX
  and also reports polls by outcome, distinct snapshots and the longest gap. Rerunning a day
  replaces that day only.
- **Silver**: one table per grain, keys and join rules in [pipeline/silver.md](../pipeline/silver.md); candidate tables for
  all sources in [pipeline/gold.md](../pipeline/gold.md). Primary focus of the course.
- **Gold**: products for named consumers, [pipeline/gold.md](../pipeline/gold.md).
- **Spark on this VM**: one job at a time (workflow steps hold a `flock`), zstd, small reader
  batches, no `partitionBy` on constants. Why: [system/storage.md](storage.md).

## Monitoring

**Monitor** (`monitor.py`, every 2 min, read-only). Checks, each with a one-line reason:

1. Receiver process alive.
2. Per feed (SIRI, vehicles): newest landing file written recently and still growing.
3. DATEX poller alive and its login accepted (skipped while idle).
4. Per DATEX feed: last successful poll and last write within two poll intervals; travel
   times and weather still changing (NPRA still publishing).
5. Data volume and root volume below `MONITOR_DISK_MAX_PCT` (90 %).

It writes the status page ([system/switchboard-style.md](switchboard-style.md)) and sends an ntfy push on state change
only (OK→FAIL, FAIL→OK).

**Metrics.** The exporter turns file names and sizes (never contents), the state files, the
monitor result, disk and memory into Prometheus metrics. Prometheus
scrapes every 30 s and keeps a year. Grafana shows one dashboard (`DAT535 Collector`),
provisioned from the repo and read-only (anonymous viewers, no login). Prometheus also
accepts OTLP pushes on localhost, so Spark jobs can report metrics later.

**What is public.** `serve_status.py` serves the status page at `/` and proxies `/grafana/`;
the tunnel exposes only it (port 8751) and the receiver (8750). Never the landing data,
Prometheus or the exporter, and no controls.

## Operations

- Ports, all on 127.0.0.1: receiver 8750, status page 8751, exporter 8752, Grafana 8753,
  Prometheus 8754.
- Deploy: the prod workflow runs the tests, then `collector/deploy.sh`, which copies
  `collector/` to `~/dat535-collector`, installs pinned, checksum-verified Prometheus and
  Grafana into `~/dat535-tools`, and restarts only the systemd user units whose files changed
  (a receiver restart means a collection gap and a re-subscribe). No repo clone on the VM.
- Setup, credentials, commands: [collector/README.md](../../collector/README.md). Capacity: [system/storage.md](storage.md).

## Decisions

- `ET-Client-Name`: `uis-dat535group21-collector` (also SIRI `RequestorRef`/`SubscriberRef`,
  and the DATEX `User-Agent`).
- The receiver runs on the VM, reachable through a reverse SSH tunnel to a public host.
- Push notifications: ntfy.sh; the topic name is the secret.
- DATEX credentials only in `~/.config/dat535/collector.env` (NPRA's terms make them
  confidential); sent over https only, redirects not followed.
- Bronze Parquet is zstd. Nothing deletes data automatically: when the disk fills we delete
  by hand, knowing what we lose ([system/storage.md](storage.md)).
- Silver parses the XML (SIRI, DATEX) with Python's `xml` package inside Spark tasks, not the
  `spark-xml` package (Spark 3.5 has no native `from_xml`).
- Every layer writes to `/data/dat535/{prod,dev}/<layer>/`, never to the root disk.

## Open decisions

- Spark's scratch space for Silver (`spark.local.dir`: temporary shuffle and sort files, not
  Silver's output). The default `/tmp` sits on the 38 GB root disk; a directory on `/data` is
  safer but slower (network-mounted). Measure a Silver run's spill first ([system/storage.md](storage.md)).
- Which Gold products to build beyond #1 ([pipeline/gold.md](../pipeline/gold.md)).
