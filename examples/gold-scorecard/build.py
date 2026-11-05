"""Package the static Gold viewer from an existing scorecard CSV and its run record.

Standard library only. No Spark, credentials, source API requests or application server.
"""

import argparse
import csv
import json
import math
import shutil
import tempfile
from pathlib import Path


COUNTS = (
    "planned_journeys", "reported_journeys", "reported_planned_journeys",
    "unreported_planned_journeys", "realtime_only_journeys", "reported_cancelled_journeys",
    "plan_cancellation_journeys", "plan_replaced_journeys", "scored_departures",
    "late_departures", "early_departures", "actual_equals_aimed_departures",
    "planned_calls", "reported_calls", "flagged_calls",
)
MEASURES = ("late_departure_pct", "median_departure_delay_s", "p90_departure_delay_s")
LABELS = ("line_id", "public_code", "line_name", "operator_name", "mode")
ASSETS = ("index.html", "styles.css", "app.js")


def payload(csv_path, record):
    if record.get("status") not in {"complete", "partial"}:
        raise ValueError("The viewer requires a published Gold run")
    with Path(csv_path).open(newline="", encoding="utf-8-sig") as source:
        rows = list(csv.DictReader(source))
    if len(rows) != record["tables"]["line_scorecard"]["rows"]:
        raise ValueError("CSV row count differs from the Gold run record")
    ids = set()
    for row in rows:
        if row["_gold_run_id"] != record["run_id"] or \
                row["_source_silver_run_id"] != record["silver_run_id"] or \
                row["operating_date"] != record["operating_date"]:
            raise ValueError("CSV lineage differs from the Gold run record")
        if not row["line_id"] or row["line_id"] in ids:
            raise ValueError("CSV contains a missing or duplicate line ID")
        ids.add(row["line_id"])
    for name in ("input_journeys", "input_calls", "planned_journeys", "reported_journeys",
                 "scored_departures", "late_departures", "actual_equals_aimed_departures"):
        if sum(int(r[name]) for r in rows) != record["totals"][name]:
            raise ValueError(f"CSV does not reconcile to Gold: {name}")
    lines = []
    for row in rows:
        if not (int(row["reported_journeys"]) or int(row["reported_calls"])):
            continue
        line = {k: row[k] or None for k in LABELS}
        line.update({k: int(row[k]) for k in COUNTS})
        line.update({k: float(row[k]) if row[k] else None for k in MEASURES})
        if any(v is not None and not math.isfinite(v) for k, v in line.items() if k in MEASURES):
            raise ValueError("CSV contains a non-finite measurement")
        lines.append(line)
    lines.sort(key=lambda r: (-r["scored_departures"], -r["reported_calls"], r["line_id"]))
    return {
        "meta": {
            "operating_date": record["operating_date"], "status": record["status"],
            "ready": record["ready"], "gold_run_id": record["run_id"],
            "silver_run_id": record["silver_run_id"], "late_seconds": record["late_seconds"],
            "total_lines": len(rows), "observed_lines": len(lines),
            **{k: record["totals"][k] for k in (
                "planned_journeys", "reported_journeys", "scored_departures", "late_departures")},
        },
        "lines": lines,
    }


def build_days(inputs, destination):
    if not inputs:
        raise ValueError("No daily Gold results supplied")
    datasets = [payload(csv_path, json.loads(Path(record_path).read_text(encoding="utf-8")))
                for csv_path, record_path in inputs]
    datasets.sort(key=lambda d: d["meta"]["operating_date"])
    dates = [d["meta"]["operating_date"] for d in datasets]
    if len(set(dates)) != len(dates):
        raise ValueError("Supply only one published Gold run per operating day")
    if len(datasets) > 1 and any(d["meta"]["status"] != "complete" or d["meta"]["ready"] is not True for d in datasets):
        raise ValueError("A multi-day view requires complete, ready Gold inputs")
    # Daily percentile values stay independent; they are never averaged.
    bundle = dict(days=datasets, period=dict(start=dates[0], end=dates[-1], available_days=len(dates)))
    data = dict(bundle, **datasets[-1])
    assets = Path(__file__).resolve().parent / "dist"
    for name in ASSETS:
        if not (assets / name).is_file():
            raise FileNotFoundError(f"Missing frontend asset: {name}")
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    for source in (assets / name for name in ASSETS):
        target = destination / source.name
        if source.resolve() != target.resolve():
            shutil.copyfile(source, target)
    # A temporary output also handles rebuilding from a CSV already in dist/.
    with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", dir=destination,
                                     prefix="_scorecard-", suffix=".tmp", delete=False) as target:
        temporary = Path(target.name)
        fields = None
        writer = None
        try:
            for csv_path, _ in inputs:
                with Path(csv_path).open(newline="", encoding="utf-8-sig") as source:
                    reader = csv.DictReader(source)
                    if fields is None:
                        fields = reader.fieldnames
                        writer = csv.DictWriter(target, fieldnames=fields, lineterminator="\n")
                        writer.writeheader()
                    elif reader.fieldnames != fields:
                        raise ValueError("Daily CSV schemas differ")
                    writer.writerows(reader)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    temporary.replace(destination / "scorecard.csv")
    encoded = json.dumps(bundle, ensure_ascii=False, allow_nan=False).replace("<", "\\u003c")
    script = "window.GOLD_SCORECARD = " + encoded + ";\n" + \
        "Object.assign(window.GOLD_SCORECARD, window.GOLD_SCORECARD.days[window.GOLD_SCORECARD.days.length - 1]);\n"
    temporary = destination / "_scorecard-data.js.tmp"
    temporary.write_text(script, encoding="utf-8")
    temporary.replace(destination / "scorecard-data.js")
    return data


def build(csv_path, record_path, destination):
    return build_days([(csv_path, record_path)], destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--csv", type=Path, required=True, action="append", help="repeat once per operating day")
    parser.add_argument("--run-record", type=Path, required=True, action="append", help="one matching record per CSV")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parent / "dist")
    args = parser.parse_args()
    if len(args.csv) != len(args.run_record):
        parser.error("Each CSV needs one matching --run-record")
    result = build_days(list(zip(args.csv, args.run_record)), args.output_dir)
    print(json.dumps({"lines": len(result["lines"]), "status": result["meta"]["status"],
                      "directory": str(args.output_dir)}))
