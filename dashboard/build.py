#!/usr/bin/env python3
"""Package the Gold dashboard: a map of hubs, transfer chances, delay between stops and road conditions.

Reads only published Gold days with pandas/pyarrow (no Spark, no server, no API calls).
Counts are summed across days; rates come from the sums, never averaged. Writes a static
site: index.html, app.js, style.css, data.js, per-hub shards and segments.js loaded on demand.
"""

import argparse
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
ASSETS = ("index.html", "app.js", "style.css")
SHARDS = 32
MIN_DEPARTURES = 20  # a hub needs this many timed departures to appear on the map
MIN_KNOWN = 20  # a line pair needs this many observed connections to be listed
MIN_SEGMENTS = 30  # a hub pair needs this many timed runs between them to be drawn
SEGMENT_TIMES = (0, 6, 9, 15, 18)  # local start hours of the time-of-day filter; together they cover the day
UNKNOWN_HUB = "__unknown_hub__"

DEPARTURES = [
    "scored_departures", "late_departures", "departures_early", "departures_on_time",
    "departures_1_3_min", "departures_3_5_min", "departures_5_10_min", "departures_10_min_plus",
    "cancelled_departures",
]
HOURLY = ["scored_departures", "late_departures", "cancelled_departures"]
PAIR = ["hub_id", "from_line_id", "from_origin_id", "to_line_id", "to_destination_id"]
# Gold's gap_change_counts as columns: minutes -11 (or less) .. +10 (or more).
GAPS = [f"gap_{i}" for i in range(22)]
CONNECTIONS = ["connections", "cancelled_connections", "unknown_connections", *GAPS]
SEGMENT = ["from_hub_id", "to_hub_id"]
SEGMENT_SUMS = ["timed_segments", "delay_change_s", "gained_1_min_plus", "recovered_1_min_plus"]
SEGMENT_PLACES = ["from_name", "from_latitude", "from_longitude", "to_name", "to_latitude", "to_longitude"]
CONDITIONS = ["mode", "hour", "precipitation", "road_surface", "road_situation", "traffic"]
# Gold's delay_counts: minutes -10 (or less) .. +30 (or more).
DELAYS = [f"delay_{i}" for i in range(41)]
# Time-of-day bands by first local hour: one line at one stop runs too rarely for single hours.
BANDS = [["Night", 0], ["Morning peak", 6], ["Daytime", 9], ["Afternoon peak", 15], ["Evening", 18]]
TABLES = (
    "stop_reliability", "transfer_reliability", "line_stop_delays",
    "segment_delay", "departure_conditions", "line_scorecard",
)


# --- Reading Gold ---


def published(root, day):
    """The day's Gold run marker, refused unless the run is complete and ready."""
    path = root / "gold" / "_runs" / f"operating_date={day}" / "latest.json"
    marker = json.loads(path.read_text())
    ready = (
        marker.get("schema_version") == 3
        and marker.get("status") == "complete"
        and marker.get("ready") is True
    )
    if not ready:
        raise ValueError(f"Gold {day} is not a complete, ready schema-3 run")
    return marker


def ready_days(root):
    """All operating days under gold/_runs/ with a complete, ready schema-3 marker."""
    days = []
    for folder in sorted((root / "gold" / "_runs").glob("operating_date=*")):
        marker_path = folder / "latest.json"
        if not marker_path.exists():
            continue
        day = folder.name.removeprefix("operating_date=")
        try:
            published(root, day)
            days.append(day)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    if not days:
        raise ValueError(f"No complete, ready schema-3 Gold days found under {root}")
    return days


def check(root, markers, table):
    """Each day's partition must hold exactly the rows of the run its marker names."""
    for day, marker in markers.items():
        if table == "departure_conditions" and table not in marker["tables"]:
            continue
        folder = root / "gold" / table / f"operating_date={day}"
        runs = pq.read_table(folder, columns=["_gold_run_id"])["_gold_run_id"]
        other_runs = set(pc.unique(runs).to_pylist()) - {marker["run_id"]}
        if len(runs) != marker["tables"][table]["rows"] or other_runs:
            raise ValueError(f"Gold {table} for {day} disagrees with its run marker")


def load(root, markers, table, columns, where=None):
    """All days of a checked Gold table as one pandas frame."""
    # `where` filters while scanning: all 4 M transfer rows at once took 5-7 GB.
    parts = [
        ds.dataset(root / "gold" / table / f"operating_date={day}", format="parquet")
        for day in markers
    ]
    return ds.dataset(parts).to_table(columns=columns, filter=where).to_pandas()


# --- Building the shards ---


def trimmed(counts):
    """[offset, counts...] without leading and trailing zero bins."""
    nonzero = np.flatnonzero(counts)
    if not len(nonzero):
        return [0]
    first, last = nonzero[0], nonzero[-1]
    return [int(first), *map(int, counts[first:last + 1])]


def line_delays(root, markers, ids):
    """Per hub and direction: every line and trip end with enough timed calls, plus all lines
    together (line ""), with a minute histogram per time-of-day band."""
    rows = load(root, markers, "line_stop_delays", ["hub_id", "direction", "line_id", "end_id", "end_name", "hour",
                                                    "calls", "cancelled_calls", "delay_counts"], ds.field("hub_id").isin(ids))
    rows[DELAYS] = np.stack(rows.pop("delay_counts").to_numpy()) if len(rows) else np.zeros((0, len(DELAYS)), int)
    rows["band"] = np.searchsorted([start for _, start in BANDS], rows["hour"], side="right") - 1
    rows = pd.concat([rows.assign(line_id="", end_id="", end_name=None), rows])
    key = ["hub_id", "direction", "line_id", "end_id"]
    totals = rows.groupby(key)[["calls", "cancelled_calls", *DELAYS]].sum().join(rows.groupby(key)["end_name"].last())
    totals = totals[totals[DELAYS].sum(axis=1) >= MIN_KNOWN]
    bands = rows.groupby([*key, "band"])[DELAYS].sum()
    bands = {k: dict(zip(g.index.get_level_values("band"), g.to_numpy()))
             for k, g in bands[bands.index.droplevel("band").isin(totals.index)].groupby(level=key)}
    out = {}
    for (hub, direction, line, end), row in totals.iterrows():
        counts = bands[(hub, direction, line, end)]
        out.setdefault(hub, {"arrival": [], "departure": []})[direction].append({
            "line": line, "end_id": end, "end": row["end_name"], "calls": int(row["calls"]),
            "cancelled": int(row["cancelled_calls"]),
            "bands": [trimmed(counts[b]) if b in counts else [0] for b in range(len(BANDS))]})
    return out, set(totals.index.get_level_values("line_id")) - {""}


def hub_shard(root, markers, hubs, hours, shard):
    """Hourly departures and listed line pairs for one shard of hubs, read on its own."""
    ids = hubs.index[hubs["shard"] == shard].to_list()

    # Each hub's departures per hour: [hour, scored, late, cancelled].
    shard_hours = hours[hours.index.get_level_values("hub_id").isin(ids)]
    out = {}
    for hub, group in shard_hours.groupby(level="hub_id"):
        rows = [[int(i[-1]), *map(int, v)] for i, v in zip(group.index, group.to_numpy())]
        out[hub] = {"hours": rows, "transfers": []}

    # The shard's connections, gap histograms as columns.
    columns = [
        *PAIR, "hour", "from_origin_name", "to_destination_name", *CONNECTIONS[:3],
        "gap_change_counts",
    ]
    rows = load(root, markers, "transfer_reliability", columns, ds.field("hub_id").isin(ids))
    if len(rows):
        gaps = np.stack(rows.pop("gap_change_counts").to_numpy())
    else:
        gaps = np.zeros((0, len(GAPS)), int)
    rows[GAPS] = gaps

    # Line pairs with enough known connections.
    by_pair = rows.groupby(PAIR)
    names = by_pair[["from_origin_name", "to_destination_name"]].last()
    pairs = by_pair[CONNECTIONS].sum().join(names)
    pairs = pairs[pairs[GAPS].sum(axis=1) >= MIN_KNOWN]

    # Their gap histograms per hour.
    by_hour = rows.groupby([*PAIR, "hour"])[GAPS].sum()
    listed = by_hour[by_hour.index.droplevel("hour").isin(pairs.index)]
    hourly = {
        key: {int(i[-1]): trimmed(v) for i, v in zip(group.index, group.to_numpy())}
        for key, group in listed.groupby(level=PAIR)
    }

    for key, row in pairs.iterrows():
        hub, from_line, origin, to_line, destination = key
        out[hub]["transfers"].append({
            "from": from_line,
            "origin_id": origin,
            "origin": row["from_origin_name"],
            "to": to_line,
            "destination_id": destination,
            "destination": row["to_destination_name"],
            "connections": int(row["connections"]),
            "cancelled": int(row["cancelled_connections"]),
            "unknown": int(row["unknown_connections"]),
            "gaps": trimmed(row[GAPS].to_numpy(dtype=np.int64)),
            "hours": hourly[key],
        })

    delays, lines = line_delays(root, markers, ids)
    for hub, content in out.items():
        content["delays"] = delays.get(hub, {"arrival": [], "departure": []})

    lines |= set(pairs.index.get_level_values("from_line_id"))
    lines |= set(pairs.index.get_level_values("to_line_id"))
    return out, lines


def segments(root, markers):
    """Hub pairs with their runs, delay change and gains summed over lines and days, as rows of
    [from, to, from lat, from lon, to lat, to lon, then the four SEGMENT_SUMS for each band of
    SEGMENT_TIMES]. The bands cover the day, so the page sums them for all day."""
    rows = load(root, markers, "segment_delay", [*SEGMENT, "hour", *SEGMENT_SUMS, *SEGMENT_PLACES])
    if rows["timed_segments"].sum() != sum(m["totals"]["timed_segments"] for m in markers.values()):
        raise ValueError("segment_delay does not reconcile to the Gold run markers")
    by_pair = rows.groupby(SEGMENT)
    pairs = by_pair[SEGMENT_SUMS].sum().join(by_pair[SEGMENT_PLACES].last())
    placed = np.logical_and.reduce([pairs[f"{end}_{axis}"].between(-limit, limit)
                                    for end in ("from", "to") for axis, limit in (("latitude", 90), ("longitude", 180))])
    pairs = pairs[(pairs["timed_segments"] >= MIN_SEGMENTS) & placed]
    bands = rows.assign(band=np.searchsorted(SEGMENT_TIMES, rows["hour"], side="right") - 1).pivot_table(
        index=SEGMENT, columns="band", values=SEGMENT_SUMS, aggfunc="sum", fill_value=0)
    bands = bands.reindex(columns=[(c, b) for b in range(len(SEGMENT_TIMES)) for c in SEGMENT_SUMS], fill_value=0)
    sums = bands.loc[pairs.index].to_numpy().round().astype(np.int64).tolist()
    return [[r["from_name"], r["to_name"], *(round(r[c], 5) for c in SEGMENT_PLACES if c.endswith("tude")), *band_sums]
            for (_, r), band_sums in zip(pairs.iterrows(), sums)]


def script(path, prefix, value):
    """Write `prefix + JSON + );`: JSONP-style scripts load from file:// and any static host."""
    data = json.dumps(value, separators=(",", ":"), ensure_ascii=False, default=str)
    path.write_text(prefix + data + ");\n")


# --- The site ---


def build(root, days, dist):
    """Write the site for the given Gold days to dist. Returns (hubs on the map, pairs listed)."""
    markers = {day: published(root, day) for day in days}
    for table in TABLES:
        check(root, markers, table)
    scored = sum(m["totals"]["scored_departures"] for m in markers.values())

    if dist.exists():
        shutil.rmtree(dist)
    (dist / "hubs").mkdir(parents=True)
    for name in ASSETS:
        shutil.copy(HERE / name, dist / name)

    # Hubs on the map.
    columns = ["hub_id", "hour", "hub_name", "hub_latitude", "hub_longitude", *DEPARTURES]
    stops = load(root, markers, "stop_reliability", columns)
    if stops["scored_departures"].sum() != scored:
        raise ValueError("stop_reliability does not reconcile to the Gold run markers")
    by_hub = stops.groupby("hub_id")
    labels = by_hub[["hub_name", "hub_latitude", "hub_longitude"]].last()
    hubs = by_hub[DEPARTURES].sum().join(labels)
    # Silver flags missing or invalid coordinates but keeps the stop; the map needs both.
    placed = hubs["hub_latitude"].between(-90, 90) & hubs["hub_longitude"].between(-180, 180)
    known = hubs.index != UNKNOWN_HUB
    busy = hubs["scored_departures"] >= MIN_DEPARTURES
    hubs = hubs[known & busy & placed].sort_values("scored_departures", ascending=False)
    hubs["shard"] = np.arange(len(hubs)) % SHARDS  # busiest hubs spread over shards
    hours = stops.groupby(["hub_id", "hour"])[HOURLY].sum()

    # One script per shard of hubs.
    used, pairs = set(), 0
    for shard in range(SHARDS):
        content, lines = hub_shard(root, markers, hubs, hours, shard)
        used |= lines
        pairs += sum(len(h["transfers"]) for h in content.values())
        script(dist / "hubs" / f"{shard:02d}.js", f"window.goldShard({shard},", content)

    # Segment delay map.
    drawn = segments(root, markers)
    script(dist / "segments.js", "window.goldSegments(", drawn)

    # Road conditions and line labels (skip days where DATEX Silver was absent).
    measures = ["scored_departures", "late_departures"]
    cond_markers = {d: m for d, m in markers.items() if "departure_conditions" in m["tables"]}
    if cond_markers:
        conditions = load(root, cond_markers, "departure_conditions", [*CONDITIONS, *measures])
        cond_scored = sum(m["totals"]["scored_departures"] for m in cond_markers.values())
        if conditions["scored_departures"].sum() != cond_scored:
            raise ValueError("departure_conditions does not reconcile to the Gold run markers")
        cube_rows = conditions.groupby(CONDITIONS, as_index=False)[measures].sum().to_numpy().tolist()
    else:
        cube_rows = []
    line_columns = ["line_id", "public_code", "line_name", "mode", "operator_name"]
    lines = load(root, markers, "line_scorecard", line_columns)
    lines = lines[lines["line_id"].isin(used)].groupby("line_id").last()

    first = next(iter(markers.values()))
    script(dist / "data.js", "window.goldData(", {
        "days": [
            {
                "date": d,
                "gold_run_id": m["run_id"],
                "silver_run_id": m["silver_run_id"],
                "datex_run_id": m["datex_run_id"],
                "totals": m["totals"],
            }
            for d, m in markers.items()
        ],
        "rules": {
            "late_seconds": first["late_seconds"],
            "max_transfer_slack_s": first["max_transfer_slack_s"],
            "weather_max_m": first["weather_max_m"],
            "min_departures": MIN_DEPARTURES,
            "min_known_connections": MIN_KNOWN,
            "min_segments": MIN_SEGMENTS,
            "segment_times": SEGMENT_TIMES,
            "max_segment_change_s": first["max_segment_change_s"],
            "gap_low_min": -11,
            "delay_low_min": -10,
            "delay_bins": len(DELAYS),
            "bands": BANDS,
            "shards": SHARDS,
        },
        "hub_columns": ["id", "name", "lat", "lon", "shard", *DEPARTURES],
        "hubs": [
            [
                hub,
                r["hub_name"],
                round(r["hub_latitude"], 5),
                round(r["hub_longitude"], 5),
                int(r["shard"]),
                *[int(r[c]) for c in DEPARTURES],
            ]
            for hub, r in hubs.iterrows()
        ],
        "lines": {
            i: [r["public_code"], r["line_name"], r["mode"], r["operator_name"]]
            for i, r in lines.iterrows()
        },
        "condition_columns": [*CONDITIONS, *measures],
        "conditions": cube_rows,
    })
    return len(hubs), pairs, len(drawn)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output-dir", type=Path, required=True, help="pipeline output root containing gold/"
    )
    parser.add_argument(
        "--date", action="append", help="operating day; repeat for several"
    )
    parser.add_argument(
        "--all-ready",
        action="store_true",
        help="include every complete, ready schema-3 Gold day under --output-dir",
    )
    parser.add_argument("--dist", type=Path, default=HERE / "dist")
    args = parser.parse_args(argv)
    if args.all_ready and args.date:
        parser.error("use either --date or --all-ready, not both")
    if not args.all_ready and not args.date:
        parser.error("pass at least one --date or --all-ready")
    days = ready_days(args.output_dir) if args.all_ready else sorted(set(args.date))
    hubs, pairs, drawn = build(args.output_dir, days, args.dist)
    sizes = sorted((p.stat().st_size, p.name) for p in args.dist.rglob("*.js"))
    total_mb = sum(s for s, _ in sizes) / 1e6
    largest_mb, largest = sizes[-1][0] / 1e6, sizes[-1][1]
    print(f"{hubs} hubs, {pairs} line pairs, {drawn} segments; {total_mb:.1f} MB, "
          f"largest {largest} {largest_mb:.1f} MB")


if __name__ == "__main__":
    main()
