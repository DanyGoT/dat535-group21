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
| `docs/system/architecture.md` | Working on the receiver, monitor, landing zone, batch jobs, or anything crossing components |
| `docs/entur/sources.md` | Fetching any Entur data: URLs, formats, refresh, rate limits, licence |
| `docs/entur/realtime.md` | Working with SIRI ET/SX: pub/sub vs polling, message content |
| `docs/entur/identifiers.md` | Joining real-time to schedule/stops, defining keys and grain in Silver |
| `entur-ai-docs/` | Entur's BigQuery SIRI-ET catalog (generated, don't edit): reference for Silver design and cross-checking results, not pipeline input |
| `docs/system/switchboard-style.md` | Building the monitor's status page (read-only) |
| `docs/problems.md` | After hitting and fixing a non-trivial problem: add an entry (raw material for the report) |

Items marked **UNVERIFIED** in the docs are research gaps, not facts. Verify against the
source (or real data) before building on them, then update the doc.

## Environment

- Runtime: UiS OpenStack VM (Ubuntu), Spark **3.5.0**, Java 8, Python venv `~/spark-env`
  (installed by `labs/lab1_install-spark.sh`).
- CI/CD: GitHub Actions on a **self-hosted runner** on the VM.
  `dev` branch → `.github/workflows/dat535-dev.yml`; `main` → `dat535-prod.yml` (manual approval).
- Pipeline code reads `DAT535_ENV` (`dev`/`prod`) and `DAT535_OUTPUT_DIR` from the environment.
- The receiver and monitor are long-running infrastructure outside GitHub Actions
  (see `docs/system/architecture.md`).

## Rules

- **Respect Entur's limits** — never hammer endpoints in tests or loops. Every request sends
  the `ET-Client-Name` header. Details in `docs/entur/sources.md`.
- **Landing/raw data is immutable.** Nothing but the receiver writes to it; nothing deletes or
  rewrites it. Batch jobs read only closed files (never `*.part`).
- `labs/` is course reference material (Spark examples, CI/CD guide). Use it to understand
  the environment, not as a code-style template.
