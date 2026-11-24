#!/usr/bin/env python3
"""Render readable Markdown summaries and multi-day Silver quality reports from run records.

Plain Python (no Spark). Reads JSON run records written by bronze.py, bronze_datex.py,
silver.py, silver_datex.py and gold.py.

    python pipeline/summary.py summary <record.json> [...]
    python pipeline/summary.py quality <file-or-dir> [...]
"""

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

ENTUR_FACTS = ("journey", "stop_call", "call_prediction", "vehicle_position", "sx_situation")
DATEX_FACTS = ("road_situation", "road_situation_interval", "road_weather", "road_travel_time")


def classify(record):
    """Identify which pipeline job produced a run record."""
    if "feeds" in record:
        return "bronze_datex" if record.get("source") == "datex" else "bronze_entur"
    if "tables" in record and "totals" in record:
        return "gold"
    if "tables" in record and record.get("source") == "datex":
        return "silver_datex"
    if "tables" in record and "operating_date" in record:
        return "silver_entur"
    return "unknown"


def record_date(record):
    return str(
        record.get("operating_date")
        or record.get("observation_date")
        or record.get("date")
        or "?"
    )


def elapsed_seconds(record):
    """Elapsed seconds from elapsed_s or started_at..finished_at, else None."""
    if record.get("elapsed_s") is not None:
        return float(record["elapsed_s"])
    started, finished = record.get("started_at"), record.get("finished_at")
    if not started or not finished:
        return None
    try:
        t0 = datetime.fromisoformat(started.replace("Z", "+00:00"))
        t1 = datetime.fromisoformat(finished.replace("Z", "+00:00"))
        return max(0.0, (t1 - t0).total_seconds())
    except ValueError:
        return None


def fmt_int(value):
    return f"{int(value):,}" if value is not None else "—"


def fmt_pct(part, whole):
    if not whole:
        return "—"
    return f"{100.0 * part / whole:.2f}%"


def fmt_duration(seconds):
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, rem = divmod(int(round(seconds)), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m" if hours else f"{minutes}m {rem:02d}s"


def status_label(record, kind):
    if kind in {"bronze_entur", "bronze_datex"}:
        feeds = record.get("feeds", {})
        bad = sum(sum(f.get("bad_rows", {}).values()) for f in feeds.values())
        skipped = sum(1 for f in feeds.values() if f.get("skipped"))
        if skipped == len(feeds) and feeds:
            return "skipped"
        return f"ok ({bad} bad)" if bad else "ok"
    status = record.get("status", "unknown")
    ready = record.get("ready")
    skipped = record.get("skipped_tables") or []
    if status == "complete" and ready:
        suffix = f", skipped: {', '.join(skipped)}" if skipped else ""
        return f"complete (ready{suffix})"
    if status == "partial":
        return "partial (not ready)"
    return status


def summarize_one(record):
    """One concise table row dict describing a single run record."""
    kind = classify(record)
    label = {
        "bronze_entur": "Bronze Entur",
        "bronze_datex": "Bronze DATEX",
        "silver_entur": "Silver Entur",
        "silver_datex": "Silver DATEX",
        "gold": "Gold",
    }.get(kind, "Unknown")

    io_summary, flagged, rejected = "—", "—", "—"
    tables = record.get("tables", {})

    if kind in {"bronze_entur", "bronze_datex"}:
        feeds = record.get("feeds", {})
        files = sum(f.get("files", 0) for f in feeds.values())
        rows = sum(f.get("rows", 0) for f in feeds.values())
        bad = sum(sum(f.get("bad_rows", {}).values()) for f in feeds.values())
        io_summary = f"{fmt_int(files)} files → {fmt_int(rows)} rows"
        if kind == "bronze_datex":
            errors = sum(f.get("polls", {}).get("fetch_error", 0) for f in feeds.values())
            flagged = f"{fmt_int(errors)} fetch_err" if errors else "0"
        rejected = fmt_int(bad)

    elif kind == "silver_entur":
        reconcile = record.get("call_reconciliation", {})
        in_v = reconcile.get("input_versions")
        calls = tables.get("stop_call", {}).get("rows")
        if in_v is not None and calls is not None:
            io_summary = f"{fmt_int(in_v)} call ver → {fmt_int(calls)} calls"
        elif calls is not None:
            io_summary = f"{fmt_int(calls)} calls"
        flagged_n = sum(tables.get(t, {}).get("flagged_rows", 0) for t in ENTUR_FACTS)
        flagged = fmt_int(flagged_n)
        q_rows = tables.get("quarantine", {}).get("rows", 0)
        rejected = fmt_int(q_rows)

    elif kind == "silver_datex":
        in_polls = sum(
            v.get("rows", 0)
            for v in record.get("inputs", {}).values()
            if isinstance(v, dict)
        )
        out_rows = sum(tables.get(t, {}).get("rows", 0) for t in DATEX_FACTS)
        io_summary = f"{fmt_int(in_polls)} polls → {fmt_int(out_rows)} facts"
        flagged_n = sum(tables.get(t, {}).get("flagged_rows", 0) for t in DATEX_FACTS)
        flagged = fmt_int(flagged_n)
        q_rows = tables.get("datex_quarantine", {}).get("rows", 0)
        rejected = fmt_int(q_rows)

    elif kind == "gold":
        totals = record.get("totals", {})
        scored = totals.get("scored_departures")
        late = totals.get("late_departures")
        calls = totals.get("input_calls")
        io_summary = f"{fmt_int(calls)} calls → {fmt_int(scored)} timed"
        flagged = f"{fmt_int(late)} late ({fmt_pct(late or 0, scored or 0)})" if scored else "—"
        rejected = "0"

    return {
        "kind": kind,
        "layer": label,
        "date": record_date(record),
        "status": status_label(record, kind),
        "io": io_summary,
        "flagged": flagged,
        "rejected": rejected,
        "elapsed": fmt_duration(elapsed_seconds(record)),
        "run_id": record.get("run_id", "?"),
        "error": record.get("error"),
        "contract_errors": record.get("contract_errors") or {},
    }


def render_step_summary(records):
    """Compact Markdown summary for GitHub Actions $GITHUB_STEP_SUMMARY."""
    lines = ["## Pipeline run", ""]
    if not records:
        lines.append("No run record written.")
        return "\n".join(lines) + "\n"

    order = {"bronze_entur": 0, "bronze_datex": 1, "silver_datex": 2, "silver_entur": 3, "gold": 4}
    rows = [summarize_one(r) for r in records]
    rows.sort(key=lambda r: (order.get(r["kind"], 9), r["date"]))

    lines += [
        "| Layer | Date | Status | In → Out | Flagged | Quarantined / Bad | Elapsed |",
        "| ----- | ---- | ------ | -------- | ------: | ----------------: | ------: |",
    ]
    for r in rows:
        lines.append(
            f"| {r['layer']} | {r['date']} | {r['status']} | {r['io']} | "
            f"{r['flagged']} | {r['rejected']} | {r['elapsed']} |"
        )

    notes = []
    for r in rows:
        if r["error"]:
            notes.append(f"- **{r['layer']} ({r['date']}) error:** `{r['error']}`")
        if r["contract_errors"]:
            notes.append(f"- **{r['layer']} ({r['date']}) contract errors:** `{r['contract_errors']}`")
    if notes:
        lines += ["", "### Issues", ""] + notes

    lines += [
        "",
        "Full run records are uploaded as workflow artifacts (`*-runs-*`).",
    ]
    return "\n".join(lines) + "\n"


def latest_per_day(records, kind):
    """Keep the newest run record per date for one layer, sorted by date."""
    by_day = {}
    for record in records:
        if classify(record) != kind:
            continue
        day = record_date(record)
        prev = by_day.get(day)
        if prev is None or str(record.get("started_at", "")) >= str(prev.get("started_at", "")):
            by_day[day] = record
    return [by_day[d] for d in sorted(by_day)]


def et_bronze_messages(record):
    et = record.get("inputs", {}).get("et", {})
    return sum(
        v.get("rows", 0)
        for k, v in et.items()
        if isinstance(v, dict) and "rows" in v and k not in {"decoded", "retained_versions"}
    )


def entur_decoded_total(record):
    total = 0
    for feed in ("et", "sx", "vehicles"):
        decoded = record.get("inputs", {}).get(feed, {}).get("decoded", {})
        total += sum(v for k, v in decoded.items() if k != "control")
    return total


def render_quality_report(records, top_operators=15):
    """Multi-day Silver quality report: funnel, coverage, dq_issue, quarantine and DATEX."""
    entur = latest_per_day(records, "silver_entur")
    datex = latest_per_day(records, "silver_datex")
    lines = ["# Silver quality report", ""]

    if not entur and not datex:
        lines.append("No Silver run records found.")
        return "\n".join(lines) + "\n"

    if entur:
        lines += [
            "## Entur Silver funnel",
            "",
            "| Operating day | Schema | Status | Bronze ET msgs (3d) | Decoded call ver | "
            "Retained call ver | Final `stop_call` | Superseded / dup removed | Elapsed |",
            "| ------------- | -----: | ------ | ------------------: | ---------------: | "
            "----------------: | ----------------: | -----------------------: | ------: |",
        ]
        for r in entur:
            decoded_calls = r.get("inputs", {}).get("et", {}).get("decoded", {}).get("call")
            rec = r.get("call_reconciliation", {})
            retained = rec.get("input_versions")
            stop_calls = r.get("tables", {}).get("stop_call", {}).get("rows")
            removed = rec.get("duplicate_versions_removed")
            lines.append(
                f"| {record_date(r)} | {r.get('schema_version', '?')} | "
                f"{status_label(r, 'silver_entur')} | {fmt_int(et_bronze_messages(r))} | "
                f"{fmt_int(decoded_calls)} | {fmt_int(retained)} | {fmt_int(stop_calls)} | "
                f"{fmt_int(removed)} | {fmt_duration(elapsed_seconds(r))} |"
            )

        lines += [
            "",
            "## Plan coverage (`stop_call`)",
            "",
            "| Operating day | Planned calls | Matched | No real-time | Real-time only | Reporting share |",
            "| ------------- | ------------: | ------: | -----------: | -------------: | --------------: |",
        ]
        for r in entur:
            cov = r.get("call_coverage", {})
            matched = cov.get("matched", 0)
            no_rt = cov.get("no_realtime", 0)
            rt_only = cov.get("realtime_only", 0)
            planned = matched + no_rt
            lines.append(
                f"| {record_date(r)} | {fmt_int(planned)} | {fmt_int(matched)} | "
                f"{fmt_int(no_rt)} ({fmt_pct(no_rt, planned)}) | {fmt_int(rt_only)} | "
                f"{fmt_pct(matched, planned)} |"
            )

        # Aggregate dq_issue across Entur fact tables and quarantine against decoded versions.
        issue_totals = Counter()
        quarantine_totals = Counter()
        total_decoded = 0
        total_quarantined = 0
        for r in entur:
            total_decoded += entur_decoded_total(r)
            tables = r.get("tables", {})
            for table in ENTUR_FACTS:
                for name, count in tables.get(table, {}).get("issues", {}).items():
                    issue_totals[(table, name)] += count
            q = tables.get("quarantine", {})
            total_quarantined += q.get("rows", 0)
            for name, count in q.get("issues", {}).items():
                quarantine_totals[name] += count

        lines += [
            "",
            f"## Entur quarantine ({len(entur)} day(s))",
            "",
            f"Total quarantined: **{fmt_int(total_quarantined)}** of **{fmt_int(total_decoded)}** "
            f"decoded non-control versions (**{fmt_pct(total_quarantined, total_decoded)}**).",
            "",
        ]
        if quarantine_totals:
            lines += [
                "| Quarantine reason | Rows | Share of decoded versions |",
                "| ----------------- | ---: | ------------------------: |",
            ]
            for reason, count in quarantine_totals.most_common():
                lines.append(f"| `{reason}` | {fmt_int(count)} | {fmt_pct(count, total_decoded)} |")

        if issue_totals:
            lines += [
                "",
                f"## Entur field-level `dq_issue` flags ({len(entur)} day(s))",
                "",
                "| Table | Rule | Flagged rows |",
                "| ----- | ---- | -----------: |",
            ]
            for (table, rule), count in issue_totals.most_common():
                lines.append(f"| `{table}` | `{rule}` | {fmt_int(count)} |")

        # Per-operator actual_departure_equals_aimed across days.
        op_dep, op_eq = Counter(), Counter()
        for r in entur:
            for item in r.get("actual_equals_aimed_by_operator", []):
                op = item.get("operator_id") or "(null)"
                op_dep[op] += item.get("actual_departures", 0)
                op_eq[op] += item.get("actual_equals_aimed", 0)
        nonzero = [
            (op_eq[op] / op_dep[op], op_eq[op], op_dep[op], op)
            for op in op_dep
            if op_dep[op] >= 100 and op_eq[op] > 0
        ]
        nonzero.sort(reverse=True)
        if nonzero:
            lines += [
                "",
                f"## Operators with highest `actual_departure_equals_aimed` (min 100 actual departures)",
                "",
                "| Operator | Actual departures | `actual == aimed` | Share |",
                "| -------- | ----------------: | ----------------: | ----: |",
            ]
            for share, eq, dep, op in nonzero[:top_operators]:
                lines.append(f"| `{op}` | {fmt_int(dep)} | {fmt_int(eq)} | {100.0 * share:.2f}% |")

    if datex:
        lines += [
            "",
            "## DATEX Silver quality",
            "",
            "| Observation day | Status | Situations | Weather rows | Travel-time rows | "
            "Flagged facts | Quarantined | Elapsed |",
            "| --------------- | ------ | ---------: | -----------: | ---------------: | "
            "------------: | ----------: | ------: |",
        ]
        datex_issues = Counter()
        datex_quarantine = Counter()
        for r in datex:
            t = r.get("tables", {})
            flagged_n = sum(t.get(name, {}).get("flagged_rows", 0) for name in DATEX_FACTS)
            q = t.get("datex_quarantine", {})
            lines.append(
                f"| {record_date(r)} | {status_label(r, 'silver_datex')} | "
                f"{fmt_int(t.get('road_situation', {}).get('rows'))} | "
                f"{fmt_int(t.get('road_weather', {}).get('rows'))} | "
                f"{fmt_int(t.get('road_travel_time', {}).get('rows'))} | "
                f"{fmt_int(flagged_n)} | {fmt_int(q.get('rows', 0))} | "
                f"{fmt_duration(elapsed_seconds(r))} |"
            )
            for name in DATEX_FACTS + ("weather_site", "road_section"):
                for rule, count in t.get(name, {}).get("issues", {}).items():
                    datex_issues[(name, rule)] += count
            for rule, count in q.get("issues", {}).items():
                datex_quarantine[rule] += count

        if datex_issues:
            lines += [
                "",
                f"### DATEX `dq_issue` flags ({len(datex)} day(s))",
                "",
                "| Table | Rule | Flagged rows |",
                "| ----- | ---- | -----------: |",
            ]
            for (table, rule), count in datex_issues.most_common():
                lines.append(f"| `{table}` | `{rule}` | {fmt_int(count)} |")

        if datex_quarantine:
            lines += [
                "",
                f"### DATEX quarantine reasons ({len(datex)} day(s))",
                "",
                "| Reason | Rows |",
                "| ------ | ---: |",
            ]
            for reason, count in datex_quarantine.most_common():
                lines.append(f"| `{reason}` | {fmt_int(count)} |")

    return "\n".join(lines) + "\n"


def load_records(paths):
    """Load JSON run records from files or directories (skipping non-run-record JSON files)."""
    files = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            files.extend(sorted(p for p in path.rglob("*.json") if p.name != "latest.json"))
        elif path.is_file():
            files.append(path)
    records = []
    for path in files:
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(data, dict) and classify(data) != "unknown":
            records.append(data)
    return records


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p_sum = sub.add_parser("summary", help="Concise Markdown table for one pipeline run")
    p_sum.add_argument("paths", nargs="*", help="Run-record JSON files or directories")

    p_qual = sub.add_parser("quality", help="Multi-day Silver quality report")
    p_qual.add_argument("paths", nargs="+", help="Run-record JSON files or directories")
    p_qual.add_argument("--top-operators", type=int, default=15)

    args = parser.parse_args(argv)
    records = load_records(args.paths)
    if args.command == "summary":
        sys.stdout.write(render_step_summary(records))
    else:
        sys.stdout.write(render_quality_report(records, top_operators=args.top_operators))
    return 0


if __name__ == "__main__":
    sys.exit(main())
