"""Snapshots of running Spark jobs' status API, for a profile of Silver's stages and joins.

Run on the VM next to a Silver job (Python 3.11, standard library, no Spark of its own):

    python3 silver_profile.py <out_dir> [hours]

Every 30 s it saves, per application on localhost:4040, the stages with their metrics, a
task-time and shuffle summary of each finished stage, the jobs, and the SQL executions with
their physical plans to <out_dir>/<app_id>.json. The UI dies with its job, so the last
snapshot can miss up to 30 s at the end. Read-only: it never touches Spark's data.

    python3 silver_profile.py summary <out_dir>/<app_id>.json

prints one row per Spark SQL execution of a snapshot as JSON: what it wrote or computed, its
joins, its time, shuffle and spill, and the most uneven of its stages (slowest task over the
median task).
"""

import json
import os
import re
import sys
import time
import urllib.request
from collections import Counter
from pathlib import Path

UI = "http://localhost:4040/api/v1/applications"
QUANTILES = "0.0,0.5,0.95,1.0"


def get(path):
    with urllib.request.urlopen(f"{UI}{path}", timeout=20) as response:
        return json.load(response)


def snapshot(app, summaries):
    base = f"/{app['id']}"
    stages = get(f"{base}/stages")
    for stage in stages:
        key = f"{stage['stageId']}.{stage['attemptId']}"
        if stage["status"] == "COMPLETE" and key not in summaries:
            summaries[key] = get(f"{base}/stages/{stage['stageId']}/{stage['attemptId']}"
                                 f"/taskSummary?quantiles={QUANTILES}")
    return {
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "app": app,
        "stages": stages,
        "task_summaries": summaries,
        "jobs": get(f"{base}/jobs"),
        "sql": get(f"{base}/sql?details=true&planDescription=true&offset=0&length=10000"),
    }


def main(out_dir, hours=3.0):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    summaries = {}  # per app id: finished stages' summaries, fetched once
    stop = time.monotonic() + float(hours) * 3600
    while time.monotonic() < stop:
        try:
            for app in get(""):
                record = snapshot(app, summaries.setdefault(app["id"], {}))
                part = out / f"{app['id']}.json.part"
                part.write_text(json.dumps(record))
                os.replace(part, out / f"{app['id']}.json")
        except OSError:
            pass  # no job running right now, or it just ended
        time.sleep(30)


def target(plan):
    """The folder an execution writes, relative to its run's staging folder, or None."""
    found = re.search(r"Execute InsertIntoHadoopFsRelationCommand\n.*?Arguments: file:(\S+?),",
                      plan, re.S)
    return re.sub(r"^.*/(_staging/[^/]+|silver)/", "", found.group(1)) if found else None


def summary(path):
    snap = json.loads(Path(path).read_text())
    jobs = {j["jobId"]: j for j in snap["jobs"]}
    stages = {s["stageId"]: s for s in snap["stages"]}
    rows = []
    for q in sorted(snap["sql"], key=lambda q: q["id"]):
        ids = [s for j in q["successJobIds"] + q["failedJobIds"] + q["runningJobIds"]
               for s in jobs[j]["stageIds"] if s in stages and j in jobs]
        mine = [stages[s] for s in ids]
        total = lambda name: sum(s[name] for s in mine)  # noqa: E731
        uneven = []
        for s in mine:
            times = snap["task_summaries"].get(f"{s['stageId']}.{s['attemptId']}", {})
            run = times.get("executorRunTime")  # quantiles 0, 0.5, 0.95, 1
            if run and s["numTasks"] >= 8 and run[1] > 0:
                uneven.append((round(run[3] / run[1], 1), s["stageId"], s["numTasks"],
                               round(run[1] / 1000, 1), round(run[3] / 1000, 1)))
        worst = max(uneven, default=None)
        rows.append({
            "id": q["id"],
            # A write names its folder; anything else its call site (e.g. "count at silver.py:462").
            "does": target(q["planDescription"]) or re.sub(r" at \S*/", " at ", q["description"]),
            "status": q["status"],
            "seconds": round(q["duration"] / 1000),
            "task_seconds": round(total("executorRunTime") / 1000),
            "shuffle_write_bytes": total("shuffleWriteBytes"),
            "memory_spilled_bytes": total("memoryBytesSpilled"),
            "disk_spilled_bytes": total("diskBytesSpilled"),
            "joins": dict(Counter(n["nodeName"] for n in q["nodes"] if "Join" in n["nodeName"])),
            "most_uneven_stage": worst and dict(zip(
                ["max_over_median", "stage", "tasks", "median_task_s", "max_task_s"], worst)),
        })
    json.dump(rows, sys.stdout, indent=1)
    print()


if __name__ == "__main__":
    if sys.argv[1] == "summary":
        summary(sys.argv[2])
    else:
        main(*sys.argv[1:])
