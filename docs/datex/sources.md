# Statens vegvesen DATEX II: access and collection

Road situations, travel times and road weather from the Norwegian Public Roads Administration
(NPRA), for explaining delays. Collected by `collector/datex.py`, Bronze in
`pipeline/bronze_datex.py`. What the payloads contain: [datex/content.md](content.md). Measured with our account
from the VM, 2026-10-23/24.

## Access and terms

- Base URL `https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/<Service>/pullsnapshotdata`.
  Pull only, HTTP GET with **Basic Auth**. Without credentials: `401`, realm `datex2/prod-1`.
- One account for the group, ordered through NPRA's access form. Username and password came in
  **two separate emails**. The form asks for a fixed IP or DNS name: give the VM's outgoing IP
  (`curl https://api.ipify.org` on the VM). Both live only in `~/.config/dat535/collector.env`
  on the VM; `python3 ~/dat535-collector/datex.py --check` tests them.
- Terms: NLOD. Username and password are confidential (never in git, never shown to end
  users). Always name NPRA as the source. **Norwegian message texts must not be translated**;
  translate DATEX codes through our own table instead. No rate limits are published.
- News and planned downtime: https://www.vegvesen.no/en/about-us/about-us/open-data/datex/information-and-news/
  (RSS available). Contact: datex@vegvesen.no.

## Feeds

| Feed (ours) | Service | Snapshot (raw / gzip) | NPRA updates | We poll every | Why |
| ----------- | ------- | --------------------: | ------------ | ------------- | --- |
| situation | `GetSituation` | 28.6 MB / 2.9 MB | continuously | 5 min | records carry their own times; only situations shorter than 5 min are missed |
| traveltime | `GetTravelTimeData` | 0.25 MB / 5 KB | every 5 min; **end of life** | 2.5 min | half the cadence, so no update is skipped |
| weather | `GetMeasuredWeatherData` | 1.5 MB / 36 KB | every 10 min | 5 min | half the cadence |
| traveltime-locations | `GetPredefinedTravelTimeLocations` | 0.6 MB / 0.16 MB | when sections change | 6 h | reference table |
| weather-sites | `GetMeasurementWeatherSiteTable` | 1.9 MB / 44 KB | when stations change | 6 h | reference table |

Not collected: road surface forecasts (only useful for predicting departures days ahead,
[pipeline/gold.md](../pipeline/gold.md)) and webcams.

## What shaped the poller

- **No `Last-Modified`, no gzip.** NPRA's spec says "some" services support
  `If-Modified-Since`; none of ours do. Every poll downloads the full snapshot, whatever
  `Accept-Encoding` says.
- **An unchanged snapshot is byte-identical** (travel time and weather, two GETs 20 s apart:
  same SHA-256). A body hash tells "new" from "same again"; the monitor uses it to notice when
  NPRA stops publishing.
- **Situations change constantly:** `publicationTime` was 17 s before the request, the newest
  record version 50 s.

## Cost per day

| Feed | Download | Landing (gzip) | Bronze (zstd) |
| ---- | -------: | -------------: | ------------: |
| situation | 8.2 GB | ~780 MB | ~520 MB |
| traveltime | 140 MB | ~3 MB | ~4 MB |
| weather | 420 MB | ~10 MB | ~13 MB |
| location tables | 10 MB | ~1 MB | ~1 MB |

One landing line per poll, failures included (format in the `datex.py` docstring). Bronze is
smaller than the gzipped landing: a situation snapshot is 27.8 MB raw, 2.72 MB gzipped,
1.79 MB in Bronze. All of DATEX is ~7 % of what we store per day ([system/storage.md](../system/storage.md)).

## End-to-end on the VM

2026-10-24 17:55–18:01 UTC, real NPRA data, scratch directory: the poller crossed an hour
boundary and closed hour 17 on time; weather polled again at 18:00 was byte-identical (not
counted as new); travel time changed between 17:55 and 17:57, situations within 5 min;
SIGTERM left the open hour as `.part`. `bronze_datex.py` read only the closed hour, flagged
hour 18 as open, wrote no bad rows and measured a 150.2 s gap between travel time polls.

Operating it (credentials, `--check`, logs): [collector/README.md](../../collector/README.md).
