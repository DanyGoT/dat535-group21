# DAT535 Group 21 — Public Transport Reliability Pipeline

## Project statement (verbatim, as chosen)

> Ingest scheduled and real-time public transport data and build datasets for analyzing
> delays, cancellations, route reliability, and punctuality. Entur provides nationwide
> Norwegian real-time feeds including trip updates, vehicle positions, and service alerts.

## Requirements

`PROJECT.md` is the course specification and grading basis. Read it directly when a task
touches scope, layers, CI/CD or the report — do not work from summaries of it. The Silver
layer carries the most weight, then Bronze, then Gold.

## Docs index (read when relevant)

| File | Read when |
| ---- | --------- |
| `docs/system/architecture.md` | Working on collectors, monitor, landing zone, batch jobs, or anything crossing components |
| `docs/system/storage.md` | Anything that stores data or runs Spark on the VM: volumes, runway, retention, memory rules |
| `docs/system/switchboard-style.md` | Building the monitor's status page (read-only) |
| `docs/pipeline/methodology.md` | Before designing or implementing any layer: decision rules for Bronze/Silver/Gold |
| `docs/pipeline/silver.md` | Any Silver work: grain, keys, RT↔plan join, dedup, pitfalls, proposed tables |
| `docs/pipeline/gold.md` | Any Gold work: use cases (departure-risk prediction first), semantics, validation |
| `docs/entur/sources.md` | Fetching any Entur data: URLs, formats, refresh, rate limits, licence |
| `docs/entur/realtime.md` | Working with SIRI ET/SX or vehicle positions: protocol, message content |
| `docs/entur/identifiers.md` | Joining real-time to plan/stops, defining keys and grain in Silver |
| `entur-ai-docs/` | Entur's BigQuery SIRI-ET catalog (generated, don't edit): reference for Silver design and cross-checking results, not pipeline input |
| `docs/datex/sources.md` | Statens vegvesen DATEX: access, terms, feeds, poll intervals, costs |
| `docs/datex/content.md` | Parsing DATEX payloads: structure, situation records, pitfalls, Silver notes |
| `docs/external/sources.md` | Weather, road traffic or other non-Entur enrichment |
| `docs/report/problems.md` | After hitting and fixing a non-trivial problem: add an entry (raw material for the report) |
| `collector/README.md` | Operating the VM: units, setup, credentials, tunnel |

Items marked **UNVERIFIED** in the docs are research gaps, not facts. Verify against the
source (or real data) before building on them, then update the doc.

## Environment

- Runtime: UiS OpenStack VM (Ubuntu), Spark **3.5.0**, Java 8, Python venv `~/spark-env`
  (installed by `labs/lab1_install-spark.sh`; its pip `pyspark` is newer, so jobs call
  `findspark.init()` to use `/opt/spark`).
- **4 vCPU, 7.9 GB RAM, root disk 38 GB (`/tmp` lives there), data volume 984 GB (`/data`).**
  One Spark job at a time; read `docs/system/storage.md` before anything memory- or disk-heavy.
- CI/CD: GitHub Actions on a **self-hosted runner** on the VM.
  `dev` branch → `.github/workflows/dat535-dev.yml`; `main` → `dat535-prod.yml` (manual approval).
- Pipeline code reads `DAT535_ENV` (`dev`/`prod`) and `DAT535_OUTPUT_DIR` from the environment.
- The collectors, monitor and retention are long-running infrastructure outside GitHub
  Actions (see `docs/system/architecture.md`). SSH to the VM worked only from the UiS network
  (2026-10-23/24); off campus it times out.

## Rules

- **Respect Entur's and NPRA's limits** — never hammer endpoints in tests or loops. Every
  Entur request sends the `ET-Client-Name` header. Details in `docs/entur/sources.md`, `docs/datex/sources.md`.
- **Landing/raw data is immutable.** Only the collectors write to it, and no file is ever
  modified. The one exception: `collector/retention.py` may delete whole old days as a last
  resort (`docs/system/storage.md`). Batch jobs read only closed files (never `*.part`).
- **DATEX credentials are confidential** (NPRA's terms): only in `~/.config/dat535/collector.env`
  on the VM, never in git, docs or logs.
- `labs/` is course reference material (Spark examples, CI/CD guide). Use it to understand
  the environment, not as a code-style template.
