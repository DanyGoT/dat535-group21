#!/usr/bin/env python3
"""Package the Gold dashboard: a map of hubs, transfer chances and road conditions.

Reads only published Gold days (Parquet and run markers) with pandas/pyarrow, as installed in
the VM's ~/spark-env. Counts are summed across days; rates are computed from the sums, never
averaged. Writes a static site: index.html, app.js, style.css, data.js and per-hub shards
loaded on demand. No Spark, no server, no source API requests.
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
UNKNOWN_HUB = "__unknown_hub__"
DEPARTURES = ["scored_departures", "late_departures", "departures_early", "departures_on_time",
              "departures_1_3_min", "departures_3_5_min", "departures_5_10_min", "departures_10_min_plus",
              "cancelled_departures"]
PAIR = ["hub_id", "from_line_id", "from_origin_id", "to_line_id", "to_destination_id"]
GAPS = [f"gap_{i}" for i in range(22)]  # Gold's gap_change_counts: minutes -11 (or less) .. +10 (or more)
CONNECTIONS = ["connections", "cancelled_connections", "unknown_connections", *GAPS]
CONDITIONS = ["mode", "hour", "precipitation", "road_surface", "road_situation", "traffic"]
DELAYS = [f"delay_{i}" for i in range(41)]  # Gold's delay_counts: minutes -10 (or less) .. +30 (or more)
# Time-of-day bands by first local hour: one line at one stop runs too rarely for single hours.
BANDS = [["Night", 0], ["Morning peak", 6], ["Daytime", 9], ["Afternoon peak", 15], ["Evening", 18]]


def published(root, day):
    """The day's Gold run marker, refused unless the run is complete and ready."""
    marker = json.loads((root / "gold" / "_runs" / f"operating_date={day}" / "latest.json").read_text())
    if marker.get("schema_version") != 3 or marker.get("status") != "complete" or marker.get("ready") is not True:
        raise ValueError(f"Gold {day} is not a complete, ready schema-3 run")
    return marker


def check(root, markers, table):
    """Each day's partition must hold exactly the rows of the run its marker names."""
    for day, marker in markers.items():
        folder = root / "gold" / table / f"operating_date={day}"
        runs = pq.read_table(folder, columns=["_gold_run_id"])["_gold_run_id"]
        if len(runs) != marker["tables"][table]["rows"] or set(pc.unique(runs).to_pylist()) - {marker["run_id"]}:
            raise ValueError(f"Gold {table} for {day} disagrees with its run marker")


def load(root, markers, table, columns, where=None):
    """All days of a checked Gold table. `where` filters while scanning, so a shard never holds
    the other shards' rows: all 4 M transfer rows at once took 5-7 GB."""
    days = ds.dataset([ds.dataset(root / "gold" / table / f"operating_date={day}", format="parquet")
                       for day in markers])
    return days.to_table(columns=columns, filter=where).to_pandas()


def trimmed(counts):
    """[offset, counts...] without leading and trailing zero bins."""
    nonzero = np.flatnonzero(counts)
    return [int(nonzero[0]), *map(int, counts[nonzero[0]:nonzero[-1] + 1])] if len(nonzero) else [0]


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
    shard_hours = hours[hours.index.get_level_values("hub_id").isin(ids)]
    out = {hub: {"hours": [[int(i[-1]), *map(int, v)] for i, v in zip(group.index, group.to_numpy())], "transfers": []}
           for hub, group in shard_hours.groupby(level="hub_id")}
    rows = load(root, markers, "transfer_reliability",
                [*PAIR, "hour", "from_origin_name", "to_destination_name", *CONNECTIONS[:3], "gap_change_counts"],
                ds.field("hub_id").isin(ids))
    gaps = np.stack(rows.pop("gap_change_counts").to_numpy()) if len(rows) else np.zeros((0, len(GAPS)), int)
    rows[GAPS] = gaps
    by_hour = rows.groupby([*PAIR, "hour"])[GAPS].sum()
    by_pair = rows.groupby(PAIR)
    pairs = by_pair[CONNECTIONS].sum().join(by_pair[["from_origin_name", "to_destination_name"]].last())
    pairs = pairs[pairs[GAPS].sum(axis=1) >= MIN_KNOWN]
    hourly = {key: {int(i[-1]): trimmed(v) for i, v in zip(group.index, group.to_numpy())}
              for key, group in by_hour[by_hour.index.droplevel("hour").isin(pairs.index)].groupby(level=PAIR)}
    for key, row in pairs.iterrows():
        out[key[0]]["transfers"].append({
            "from": key[1], "origin_id": key[2], "origin": row["from_origin_name"],
            "to": key[3], "destination_id": key[4], "destination": row["to_destination_name"],
            "connections": int(row["connections"]), "cancelled": int(row["cancelled_connections"]),
            "unknown": int(row["unknown_connections"]), "gaps": trimmed(row[GAPS].to_numpy(dtype=np.int64)),
            "hours": hourly[key]})
    delays, lines = line_delays(root, markers, ids)
    for hub, content in out.items():
        content["delays"] = delays.get(hub, {"arrival": [], "departure": []})
    return out, lines | set(pairs.index.get_level_values("from_line_id")) | set(pairs.index.get_level_values("to_line_id"))


def script(path, prefix, value):
    # JSONP-style scripts load from file:// as well as from any static host.
    path.write_text(prefix + json.dumps(value, separators=(",", ":"), ensure_ascii=False, default=str) + ");\n")


def build(root, days, dist):
    """Write the site for the given Gold days to dist. Returns (hubs on the map, line pairs listed)."""
    markers = {day: published(root, day) for day in days}
    for table in ("stop_reliability", "transfer_reliability", "line_stop_delays", "departure_conditions", "line_scorecard"):
        check(root, markers, table)
    scored = sum(m["totals"]["scored_departures"] for m in markers.values())
    if dist.exists():
        shutil.rmtree(dist)
    (dist / "hubs").mkdir(parents=True)
    for name in ASSETS:
        shutil.copy(HERE / name, dist / name)

    stops = load(root, markers, "stop_reliability",
                 ["hub_id", "hour", "hub_name", "hub_latitude", "hub_longitude", *DEPARTURES])
    if stops["scored_departures"].sum() != scored:
        raise ValueError("stop_reliability does not reconcile to the Gold run markers")
    by_hub = stops.groupby("hub_id")
    hubs = by_hub[DEPARTURES].sum().join(by_hub[["hub_name", "hub_latitude", "hub_longitude"]].last())
    # Silver flags missing or invalid coordinates but keeps the stop; the map needs both.
    placed = hubs["hub_latitude"].between(-90, 90) & hubs["hub_longitude"].between(-180, 180)
    hubs = hubs[(hubs.index != UNKNOWN_HUB) & (hubs["scored_departures"] >= MIN_DEPARTURES) & placed]
    hubs = hubs.sort_values("scored_departures", ascending=False)
    hubs["shard"] = np.arange(len(hubs)) % SHARDS  # busiest hubs spread over shards
    hours = stops.groupby(["hub_id", "hour"])[["scored_departures", "late_departures", "cancelled_departures"]].sum()

    used, pairs = set(), 0
    for shard in range(SHARDS):
        content, lines = hub_shard(root, markers, hubs, hours, shard)
        used |= lines
        pairs += sum(len(h["transfers"]) for h in content.values())
        script(dist / "hubs" / f"{shard:02d}.js", f"window.goldShard({shard},", content)

    conditions = load(root, markers, "departure_conditions", [*CONDITIONS, "scored_departures", "late_departures"])
    if conditions["scored_departures"].sum() != scored:
        raise ValueError("departure_conditions does not reconcile to the Gold run markers")
    cube = conditions.groupby(CONDITIONS, as_index=False)[["scored_departures", "late_departures"]].sum()
    lines = load(root, markers, "line_scorecard", ["line_id", "public_code", "line_name", "mode", "operator_name"])
    lines = lines[lines["line_id"].isin(used)].groupby("line_id").last()
    first = next(iter(markers.values()))
    script(dist / "data.js", "window.goldData(", {
        "days": [{"date": d, "gold_run_id": m["run_id"], "silver_run_id": m["silver_run_id"],
                  "datex_run_id": m["datex_run_id"], "totals": m["totals"]} for d, m in markers.items()],
        "rules": {"late_seconds": first["late_seconds"], "max_transfer_slack_s": first["max_transfer_slack_s"],
                  "weather_max_m": first["weather_max_m"], "min_departures": MIN_DEPARTURES,
                  "min_known_connections": MIN_KNOWN, "gap_low_min": -11, "delay_low_min": -10, "delay_bins": len(DELAYS),
                  "bands": BANDS,
                  "shards": SHARDS},
        "hub_columns": ["id", "name", "lat", "lon", "shard", *DEPARTURES],
        "hubs": [[hub, r["hub_name"], round(r["hub_latitude"], 5), round(r["hub_longitude"], 5), int(r["shard"]),
                  *[int(r[c]) for c in DEPARTURES]] for hub, r in hubs.iterrows()],
        "lines": {i: [r["public_code"], r["line_name"], r["mode"], r["operator_name"]] for i, r in lines.iterrows()},
        "condition_columns": [*CONDITIONS, "scored_departures", "late_departures"],
        "conditions": cube.to_numpy().tolist(),
    })
    return len(hubs), pairs


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-dir", type=Path, required=True, help="pipeline output root containing gold/")
    parser.add_argument("--date", action="append", required=True, help="operating day; repeat for several")
    parser.add_argument("--dist", type=Path, default=HERE / "dist")
    args = parser.parse_args(argv)
    hubs, pairs = build(args.output_dir, sorted(set(args.date)), args.dist)
    sizes = sorted((p.stat().st_size, p.name) for p in args.dist.rglob("*.js"))
    print(f"{hubs} hubs, {pairs} line pairs; {sum(s for s, _ in sizes) / 1e6:.1f} MB, "
          f"largest {sizes[-1][1]} {sizes[-1][0] / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
