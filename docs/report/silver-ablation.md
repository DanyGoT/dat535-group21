# Is Silver a good base? An ablation

Kept off `main`: an experiment, not part of the pipeline. Script:
[`pipeline/silver_ablation.py`](../../pipeline/silver_ablation.py), run by hand.

Before building the main product, **departure risk** ([pipeline/gold.md](../pipeline/gold.md)),
we tested whether Silver can carry it: "will this departure be at least 3 minutes late?", asked 10
or 30 minutes before. One gradient-boosted model per set of feature groups, each group from one
Silver source; trained on 2026-11-02/03, tested on 11-04 (20 % of journeys, 325,629 departures),
with the stop-level history from 10-28 to 11-01. A feature counts only if it had arrived by the
time of the question, and the run fails if any join changes the number of departures.

| Feature groups | AUC, 10 min before | AUC, 30 min before |
| -------------- | -----------------: | -----------------: |
| calendar (hour, day, mode, stop order) | 0.709 | 0.707 |
| + history (`stop_call`: late share per line, stop, hour) | 0.820 | 0.819 |
| + realtime (`call_prediction`: Entur's estimate then) | 0.832 | 0.754 |
| calendar + history + realtime | 0.879 | 0.833 |
| all, adding DATEX weather, situations, congestion and SX alerts | 0.880 | 0.833 |
| all but realtime / all but history | 0.818 / 0.839 | 0.818 / 0.761 |
| all but weather, situations, congestion or alerts | 0.8795 to 0.8800 | 0.8327 to 0.8335 |

What it says: the Entur tables carry the signal, and they complement each other. Entur's estimate
matters most just before departure, the history further ahead. The DATEX joins work (79 % of
departures have a reading from their nearest weather station, 15 % a measured road section, 10 %
a situation within 500 m) and the late share does follow congestion (42 % on congested roads, 30 %
on free ones), but none of it adds to history and Entur's estimate for this question, in this
November week.

Evidence: [`evidence/silver-ablation-lead10-2026-11-04.json`](evidence/silver-ablation-lead10-2026-11-04.json),
[`evidence/silver-ablation-lead30-2026-11-04.json`](evidence/silver-ablation-lead30-2026-11-04.json).
The first run was wrong because of a Spark bug in joins with cached data
([`problems.md`](problems.md), 2026-11-07).

## Running it

It reads `silver/` under `$DAT535_OUTPUT_DIR` and writes `silver/_ablation/<run>/` there, so point
it at a dev output, never at prod. The runs above read a week of Silver backfilled by hand into a
scratch directory (deleted since). Take the Spark lock, like every job
([`docs/system/storage.md`](../system/storage.md)):

```bash
source ~/spark-env/bin/activate
export DAT535_ENV=dev DAT535_OUTPUT_DIR=/data/dat535/dev
flock /data/dat535/.spark.lock python pipeline/silver_ablation.py \
  --profile-days 2026-10-28,2026-10-29,2026-10-30,2026-10-31,2026-11-01 \
  --train-days 2026-11-02,2026-11-03 --test-days 2026-11-04 --lead 10   # and --lead 30
```

Each run took about 10 minutes on the VM.
