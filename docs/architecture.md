# Architecture

Entur has no public archive of past real-time data (see `entur/sources.md`), so we collect
it ourselves. A small always-on receiver stores raw pushes; scheduled Spark batch jobs turn
them into Medallion layers.

```
Entur SIRI ET/SX ──push──▶ receiver ──append──▶ landing/ (raw, hourly JSONL)
Entur NeTEx + NSR ─daily download──────────────▶ landing/ (raw zips)
                                                    │
                     monitor (read-only) ◀──────────┤──▶ main page + push notifications
                     exporter (read-only) ◀─────────┤──▶ Prometheus ──▶ Grafana (/grafana/)
                                                    ▼
                      Spark batch (GitHub Actions, scheduled): Bronze → Silver → Gold
```

## Components

### Receiver (always on)

- Holds a SIRI 2.0 pub/sub subscription with Entur and accepts pushed XML over HTTP.
- Deliberately dumb: **no parsing, no filtering**. Each push becomes one line in the current
  hourly file. The raw XML is kept unchanged inside the line.
- Runs on the VM, or on a separate always-on machine (then files are synced to the VM).
- Subscription request, heartbeat (30–60 s), renewal and failure handling: see
  `entur/realtime.md` (verified against `entur/anshar`). The endpoint must be reachable
  from the internet.

### Daily static download

- Once per day (Entur's limit): national NeTEx and NSR stops, stored unchanged by date.
- Required, not optional: Entur keeps at most 48 h of past data in the static export, so
  without our own copy we lose the planned schedule for older days and cannot compute delays.

### Monitor (read-only)

Runs periodically (cron/systemd timer). Only reads; never modifies landing data.
Checks:

1. Receiver process alive.
2. Current `*.part` file modified within the last N minutes.
3. Current `*.part` file has grown since the last check.
4. Disk usage of the landing volume below a threshold.

Any failure → non-zero exit plus a one-line reason.

### Main page and dashboards

- The monitor writes the main page (static HTML/JSON) to its own directory: the checks,
  problems, a few embedded Grafana panels and subscription details. Viewable from a phone.
- Push notifications are sent on **state change** only (OK→FAIL, FAIL→OK), not on every run.
- Look and structure of the main page: `switchboard-style.md`.
- The **exporter** turns the landing zone (file names and sizes only), the receiver state,
  the monitor result and disk usage into Prometheus metrics. **Prometheus** scrapes it every
  30 s and keeps a year of history. **Grafana** shows one dashboard (`DAT535 Collector`):
  stored volume, ingest rate, freshness, per-day size and hour coverage, checks and disk.
- Prometheus has its OTLP receiver on (`/api/v1/otlp/v1/metrics`, localhost only), so
  OpenTelemetry instrumentation or the Spark layers can push metrics later into the same
  store; add panels or dashboards next to `collector.json`.
- Grafana is read-only: anonymous viewers, no login form, no admin user. The datasource and
  dashboards are provisioned from the repo, so changes go through git.
- One front server (`serve_status.py`) serves the main page at `/` and proxies `/grafana/`.
  This is what a tunnel may expose: never the landing data, Prometheus or the exporter, and
  no controls.

### Batch pipeline (Spark)

- Runs on the self-hosted runner via the scheduled workflow, the morning after each
  operating day (operating days cross midnight).
- Bronze: closed hourly files → compacted Parquet per day, plus ingestion metadata
  (source file, received_at, run id).
- Silver: collapse repeated SIRI updates to the final state per journey × operating date ×
  stop; join to the archived schedule and stops; flag/quarantine invalid rows. Primary focus.
- Gold: reliability/punctuality aggregates for a concrete consumer (to be defined).
- Collection gaps (from monitor/heartbeat data) are reported as a per-day coverage metric.

## Landing zone layout

```
landing/
├── siri-et/date=YYYY-MM-DD/hour=HH.jsonl.gz   (hour=HH.jsonl.part while open)
├── siri-sx/date=YYYY-MM-DD/hour=HH.jsonl.gz   (hour=HH.jsonl.part while open)
├── netex/date=YYYY-MM-DD/rb_norway-aggregated-netex.zip
└── nsr/date=YYYY-MM-DD/Current_latest.zip
```

- **Hourly rotation:** the receiver writes `hour=HH.jsonl.part`. When the hour ends, a
  background thread in the receiver gzips it to `hour=HH.jsonl.gz.tmp`, renames that to
  `hour=HH.jsonl.gz` (atomic, so readers see only complete files) and removes the `.part`.
  Closed files are never touched again. Batch jobs read `*.jsonl.gz` and ignore `*.part`
  and `*.tmp`. Hours closed before compression was added (2026-10-15) are plain `*.jsonl`;
  readers must accept both.
  Batch jobs ignore `*.part`.
- **Line format (JSON Lines):** `{"received_at": "<ISO-8601 UTC>", "endpoint": "<et|sx>", "payload": "<raw XML string>"}`.
  Multi-line XML can't be concatenated into one valid file; JSON Lines can.
- **Why hourly:** many tiny files are slow for Spark to list and schedule, and one huge file
  can't be partitioned, rerun per day, or safely read while being written. 24 files per day
  per feed is a good balance. Gzip is not splittable, which is fine at this file size.
- `date=` and `hour=` are **UTC** (from `received_at`), so DST never produces a duplicate or
  missing hour. Operating days are Norwegian local time; Silver maps between them.
- Heartbeats are stored as ordinary lines (`HeartbeatNotification` payload). They keep the
  `.part` growing at night and give the coverage metric its raw data.
- Daily volume is unknown until measured.

## Implementation

`collector/` (Python standard library only; systemd units and example env in `collector/systemd/`):

- `receiver.py`: receiver + subscription keeper. Unset `RECEIVER_PUBLIC_URL` = receive only.
- `monitor.py`: the four checks; writes `status/index.html` + `status.json`; ntfy on change.
- `serve_status.py`: front server. GET/HEAD-only for the status directory; proxies `/grafana/`
  to Grafana (GET/HEAD/POST, no websockets). The tunnel points here.
- `exporter.py`: read-only `/metrics` for Prometheus.
- `observability/`: Prometheus config, Grafana config, provisioning and dashboard JSON.
- Ports, all bound to 127.0.0.1: receiver 8750, front server 8751, exporter 8752,
  Grafana 8753, Prometheus 8754.
- Tests: `python3 -m unittest discover -s collector/tests` (localhost only, never Entur).
- Deploy: the prod workflow tests, then runs `collector/deploy.sh`, which copies the code to
  `~/dat535-collector` on the VM, installs pinned, checksum-verified Prometheus and Grafana
  releases into `~/dat535-tools`, and (re)starts systemd **user** units. Each service restarts
  only when its own files changed. Prometheus and Grafana data live in `~/.local/state/dat535/`.
  No repo clone on the VM; one-time setup in `collector/README.md`.

## Decisions

- `ET-Client-Name`: `uis-dat535group21-collector` (also used as SIRI `RequestorRef`/`SubscriberRef`).
- Push notifications: ntfy.sh; the topic name is the secret.

## Open decisions

- XML parsing in Spark 3.5 (native `from_xml` only arrives in Spark 4.0): `spark-xml`
  package vs Python XML parsing in `mapPartitions`.
- Where the receiver runs (VM vs other machine) and how files reach the VM.
- Gold consumer and questions.
