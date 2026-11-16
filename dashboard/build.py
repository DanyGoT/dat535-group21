#!/usr/bin/env python3
"""Package the Gold dashboard: a map of hubs, transfer chances and road conditions.

Reads only published Gold days with pandas/pyarrow (no Spark, no server, no API calls).
Counts are summed across days; rates come from the sums, never averaged. Writes a static
site: index.html, app.js, style.css, data.js and per-hub shards loaded on demand.
"""

import argparse
import json
import shutil
from pathlib import Path

import numpy as np
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
ASSETS = ("index.html", "app.js", "style.css")
SHARDS = 32
MIN_DEPARTURES = 20  # a hub needs this many timed departures to appear on the map
MIN_KNOWN = 20  # a line pair needs this many observed connections to be listed
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
CONDITIONS = ["mode", "hour", "precipitation", "road_surface", "road_situation", "traffic"]
TABLES = ("stop_reliability", "transfer_reliability", "departure_conditions", "line_scorecard")


# --- Reading Gold ---


def published(root, day):
    """The day's Gold run marker, refused unless the run is complete and ready."""
    path = root / "gold" / "_runs" / f"operating_date={day}" / "latest.json"
    marker = json.loads(path.read_text())
    ready = (
        marker.get("schema_version") == 2
        and marker.get("status") == "complete"
        and marker.get("ready") is True
    )
    if not ready:
        raise ValueError(f"Gold {day} is not a complete, ready schema-2 run")
    return marker


def check(root, markers, table):
    """Each day's partition must hold exactly the rows of the run its marker names."""
    for day, marker in markers.items():
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

    lines = set(pairs.index.get_level_values("from_line_id"))
    lines |= set(pairs.index.get_level_values("to_line_id"))
    return out, lines


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

    # Road conditions and line labels.
    measures = ["scored_departures", "late_departures"]
    conditions = load(root, markers, "departure_conditions", [*CONDITIONS, *measures])
    if conditions["scored_departures"].sum() != scored:
        raise ValueError("departure_conditions does not reconcile to the Gold run markers")
    cube = conditions.groupby(CONDITIONS, as_index=False)[measures].sum()
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
            "gap_low_min": -11,
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
        "conditions": cube.to_numpy().tolist(),
    })
    return len(hubs), pairs


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output-dir", type=Path, required=True, help="pipeline output root containing gold/"
    )
    parser.add_argument(
        "--date", action="append", required=True, help="operating day; repeat for several"
    )
    parser.add_argument("--dist", type=Path, default=HERE / "dist")
    args = parser.parse_args(argv)

    hubs, pairs = build(args.output_dir, sorted(set(args.date)), args.dist)
    sizes = sorted((p.stat().st_size, p.name) for p in args.dist.rglob("*.js"))
    total_mb = sum(s for s, _ in sizes) / 1e6
    largest_mb, largest = sizes[-1][0] / 1e6, sizes[-1][1]
    print(f"{hubs} hubs, {pairs} line pairs; {total_mb:.1f} MB, "
          f"largest {largest} {largest_mb:.1f} MB")


if __name__ == "__main__":
    main()
