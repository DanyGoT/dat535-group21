# DAT535 Group 21: public transport reliability

A Spark pipeline (Medallion: Bronze → Silver → Gold) over Norwegian public transport data:
which departures run late or get cancelled, and why. We collect Entur's real-time feeds
(SIRI ET and SX, vehicle positions) and Statens vegvesen's road data (DATEX: road works,
accidents, road weather) around the clock, because neither source keeps the raw history.
The main Gold product answers a traveller's question: *will my departure be on time?*

Course spec and grading basis: [`PROJECT.md`](PROJECT.md).

## Status

| Part | State |
| ---- | ----- |
| SIRI ET/SX and vehicle position collection | running on the VM since 2026-10-15 / 10-17 |
| DATEX collection | built and tested against NPRA; credentials are on the VM, starts with the next deploy |
| NeTEx and NSR daily archive | first day archived 2026-10-26; daily timer starts with the next deploy |
| Monitoring: status page, ntfy alerts, Prometheus, Grafana | running |
| Bronze (SIRI, vehicles, DATEX) | SIRI daily at 03:00 UTC; vehicles and DATEX added |
| Silver | Entur implemented; complete 2026-10-29 validated on the VM, regular deployment pending; [contracts](docs/pipeline/silver.md) · [run evidence](docs/report/evidence/silver-week-2026-10-26-to-2026-11-01.md) |
| Gold | full-day [line scorecard example](docs/pipeline/gold-example.md) with a [simple frontend](examples/gold-scorecard/README.md); other [use cases](docs/pipeline/gold.md) remain future work |

## Docs

Start with [`docs/system/architecture.md`](docs/system/architecture.md).

| Folder | Docs |
| ------ | ---- |
| [`docs/system/`](docs/system/) | [architecture](docs/system/architecture.md): components, landing zone, pipeline, monitoring · [storage](docs/system/storage.md): VM capacity, volumes, runway, manual cleanup, Spark memory rules · [switchboard-style](docs/system/switchboard-style.md): look of the status page |
| [`docs/pipeline/`](docs/pipeline/) | [methodology](docs/pipeline/methodology.md): how we decide, layer by layer · [silver](docs/pipeline/silver.md): contracts and operation · [Silver log](docs/pipeline/silver-design-log.md): decisions, code snippets and evidence · [gold](docs/pipeline/gold.md): use cases, the prediction product |
| [`docs/entur/`](docs/entur/) | [sources](docs/entur/sources.md): every Entur source, limits, licence · [realtime](docs/entur/realtime.md): SIRI pub/sub and vehicle positions · [identifiers](docs/entur/identifiers.md): ids and join keys |
| [`docs/datex/`](docs/datex/) | [sources](docs/datex/sources.md): access, feeds, collection, costs · [content](docs/datex/content.md): what the payloads contain, pitfalls |
| [`docs/external/`](docs/external/) | [sources](docs/external/sources.md): weather, traffic and other enrichment |
| [`docs/report/`](docs/report/) | [problems](docs/report/problems.md): problems we hit, with evidence, for the report's reflection |
| [`collector/`](collector/README.md) | operating the VM: units, setup, commands, credentials, tunnel |

## Repository

| Path | Contents |
| ---- | -------- |
| `collector/` | always-on collectors, monitor, exporter, systemd units, Grafana dashboard |
| `pipeline/` | Bronze jobs, Entur Silver (`silver.py`, `silver_parse.py`, `silver_reference.py`), manual Gold scorecard (`gold_scorecard.py`), offline tests |
| `.github/workflows/` | `dat535-dev.yml` (tests + partial Bronze/Silver), `dat535-prod.yml` (approved deploy; daily Bronze + Entur Silver) |
| `notebooks/` | exploration (`peek.ipynb`) |
| `entur-ai-docs/` | Entur's generated BigQuery catalog (reference, don't edit) |
| `labs/` | course lab material |

## Tests

```sh
uv run --python 3.11 --with-requirements requirements-dev.txt --with websockets==10.4 \
  python -m unittest discover -s collector/tests    # localhost only, matches the VM's websockets
uv run --python 3.11 --with-requirements requirements-dev.txt \
  python -m unittest discover -s pipeline/tests     # needs Java 8/17; no external services
```

Both run in CI on every push and PR to `dev`, and before every deploy. On the VM the
pipeline suite uses `~/spark-env` under the shared Spark lock. Local collector tests need
the VM's websocket version: newer websocket APIs reject its `extra_headers` argument.

## Data credits

Contains data under the Norwegian Licence for Open Government Data (NLOD) made available by
Entur AS, and by the Norwegian Public Roads Administration (Statens vegvesen).
