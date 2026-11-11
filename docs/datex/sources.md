# Statens vegvesen DATEX II: access and collection

This page covers access, feed cadence and collection costs for NPRA's road data. Measurements below come from
our VM account on 2026-10-23/24.

Up: [overview](../report/overview.md) · Related: [payload content](content.md),
[external sources](../external/sources.md), [collector runbook](../../collector/README.md)

## Feeds

| Feed | Service | Snapshot raw / gzip | NPRA refresh | Our poll |
| ---- | ------- | ------------------: | ------------ | -------: |
| situation | `GetSituation` | 28.6 MB / 2.9 MB | continuous | 5 min |
| traveltime | `GetTravelTimeData` | 0.25 MB / 5 KB | 5 min, end of life | 2.5 min |
| weather | `GetMeasuredWeatherData` | 1.5 MB / 36 KB | 10 min | 5 min |
| traveltime-locations | `GetPredefinedTravelTimeLocations` | 0.6 MB / 0.16 MB | section changes | 6 h |
| weather-sites | `GetMeasurementWeatherSiteTable` | 1.9 MB / 44 KB | station changes | 6 h |

[`datex.py`](../../collector/datex.py) has collected all five since 2026-10-28.
Their [Bronze](../../pipeline/bronze_datex.py) and [Silver](../../pipeline/silver_datex.py)
jobs run in the [nightly workflow](../../.github/workflows/dat535-prod.yml).

Travel time and weather are sampled twice per publication cycle to reduce missed updates.
Situation timestamps preserve creation/version history, but events shorter than 5 min can
be missed. Outages can lose longer events too. Reference tables change slowly.

Road surface forecasts are only useful for predicting departures days ahead
([departure-risk plan](../pipeline/gold.md#1-departure-risk-primary)). They and webcams are excluded.

## Access and terms

Base URL:
`https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/<Service>/pullsnapshotdata`.

Use HTTP GET with Basic Auth. The poller sends `User-Agent` and `Accept-Encoding: gzip`.
Unauthenticated requests returned `401`, realm
`datex2/prod-1`. Our group account came through NPRA's access form, with username and
password in two separate emails. The form requests a fixed IP or DNS name. Use the VM's
outgoing IP, obtainable through `https://api.ipify.org`.

Credentials belong only in `~/.config/dat535/collector.env` on the VM, never git, logs or
end-user output. The [runbook](../../collector/README.md#datex-credentials) covers setup, logs and `--check`.

NLOD requires naming NPRA. Norwegian message texts must not be translated. Translate
DATEX codes through our own table instead. No rate limits are published.

News/downtime, with RSS:
https://www.vegvesen.no/en/about-us/about-us/open-data/datex/information-and-news/.
Contact: datex@vegvesen.no.

## What shaped the poller

- None of our feeds supplied `Last-Modified` or gzip, despite the spec's conditional-request
  support for "some" services. Polls downloaded full snapshots regardless of `Accept-Encoding`.
- Travel time and weather were byte-identical across unchanged GETs 20 s apart, with equal
  SHA-256. The monitor uses body hashes to detect stalled publishing.
- Situations changed constantly. `publicationTime` was 17 s before one request, the newest
  record version was 50 s before it.

## Cost per day

| Feed | Download | Landing gzip | Bronze zstd |
| ---- | -------: | -----------: | ----------: |
| situation | 8.2 GB | ~780 MB | ~520 MB |
| traveltime | 140 MB | ~3 MB | ~4 MB |
| weather | 420 MB | ~10 MB | ~13 MB |
| location tables | 10 MB | ~1 MB | ~1 MB |

Every poll, including failures, produces one landing line. The envelope is documented in
[`datex.py`](../../collector/datex.py). One situation snapshot measured 27.8 MB raw,
2.72 MB gzipped and 1.79 MB in Bronze. DATEX totals ~7 % of daily storage
([2026-10-24/26 baseline](../system/storage.md#what-a-day-costs)).

## End-to-end on the VM

The scratch test used real NPRA data on 2026-10-24, 17:55–18:01 UTC.

- Hour 17 closed on time. SIGTERM left the current hour as `.part`.
- Weather repeated unchanged at 18:00. Travel time changed between 17:55 and 17:57,
  and situations within 5 min.
- [`bronze_datex.py`](../../pipeline/bronze_datex.py) read the closed hour, flagged hour 18
  open, wrote no bad rows and measured 150.2 s between travel-time polls.
