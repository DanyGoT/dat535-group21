# Gold dashboard

A static page over published Gold days: a map of hubs coloured by late departures, a transfer
explorer ("with N spare minutes, how often did this connection hold?") and a comparison of late
departures under road conditions. Definitions are in the [Gold contract](../docs/pipeline/gold-example.md).

| File | Role |
| ---- | ---- |
| [`build.py`](build.py) | Reads Gold Parquet and run markers, checks lineage and totals, writes `dist/` |
| [`index.html`](index.html), [`app.js`](app.js), [`style.css`](style.css) | The page, copied into `dist/` |

## Build

On the VM, after `pipeline/gold.py` has published each day:

```sh
flock -n /data/dat535/.spark.lock ~/spark-env/bin/python dashboard/build.py --output-dir /data/dat535/prod \
  --date 2026-11-05 --date 2026-11-08 --date 2026-11-09
```

It needs pandas and pyarrow from `~/spark-env`, no Spark. It holds the Spark lock anyway, so no
Gold rerun replaces tables mid-build and no Spark job competes for memory: three days took 170 s
and peaked at 2.1 GiB RSS. `dist/` (about 55 MB, 9 MB gzipped) is
generated and not tracked in git.

## View

Open `dist/index.html` in a browser, or serve the folder from any static host:

```sh
uv run --no-project python -m http.server -d dashboard/dist 8799
```

The page loads Leaflet from cdnjs, the font from Google Fonts and map tiles from
openstreetmap.org. Data comes from Entur and Statens vegvesen under NLOD, aggregated by this
project.
