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
~/spark-env/bin/python dashboard/build.py --output-dir /data/dat535/prod \
  --date 2026-11-05 --date 2026-11-08 --date 2026-11-09
```

It needs pandas and pyarrow from `~/spark-env`, no Spark. Three days took 166 s and peaked at
2.1 GiB RSS, so run it when no Spark job holds the memory. `dist/` (about 49 MB, 9 MB gzipped) is
generated and not tracked in git.

## View

Open `dist/index.html` in a browser, or serve the folder from any static host:

```sh
uv run --no-project python -m http.server -d dashboard/dist 8799
```

The page loads Leaflet from cdnjs, the font from Google Fonts and map tiles from
openstreetmap.org. Data comes from Entur and Statens vegvesen under NLOD, aggregated by this
project.
