# External sources (non-Entur)

This page records source choices for weather, congestion and road-event enrichment. Provider checks used single
requests and terms on 2026-10-22. DATEX measurements followed on 2026-10-23/26.

Up: [overview](../report/overview.md) · Related: [DATEX access](../datex/sources.md),
[DATEX content](../datex/content.md), [Gold proposals](../pipeline/gold.md)

## Overview

| Provider | Content / history | Access | Decision |
| -------- | ----------------- | ------ | -------- |
| NPRA DATEX II 3.1 | road events, travel time/free-flow, road weather / no history | free account, obtained | collect continuously |
| MET Norway Frost | observed station weather, including coastal / history | free client id as Basic Auth user, otherwise 401 | reserve for ferries |
| NPRA Trafikkdata | hourly vehicle counts / history, ~3 h behind | free, no key, tested OK | skip, counts do not measure delay |
| Kystverket AIS | ship positions / no history | free, no registration | skip, Entur supplies ferry positions |
| Google Maps Routes | traffic travel times / no history | 5–10k free/month, then paid | reject, caching terms prevent a dataset |

DATEX serves proposed Gold products 1, 3, 4 and 5. Frost and AIS relate to product 5.
Road weather stations are a median 5.5 km from buses, but 14 km from ferries and inland.
These are rounded summaries of the [2026-10-26 08 UTC sample](../datex/content.md#does-it-line-up-with-transit).
Situations were within 2 km of half the buses in that sample.

DATEX has been collected since 2026-10-28 and has Bronze and Silver. It adds road surface
temperature/condition beyond Frost. Whether it explains delay remains untested.
Gold products 4 and 5 may find weak effects. Frost waits for the ferry/wind product.

## Endpoints

| Source | Endpoint |
| ------ | -------- |
| DATEX | `https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/<Service>/pullsnapshotdata` |
| Frost | `https://frost.met.no/observations/v0.jsonld` |
| Trafikkdata | GraphQL POST `https://trafikkdata-api.atlas.vegvesen.no`, `trafficData { volume { byHour } }` |
| AIS | raw NMEA over TCP `153.44.253.27:5631` |

## Collection and joins

Frost history permits daily fetching and backfilling to our SIRI start.
DATEX and SIRI need continuous collection. DATEX travel time measures congestion where covered.

Uncollected NPRA road surface or MET location forecasts could serve departures days ahead,
[Gold product 1](../pipeline/gold.md#1-departure-risk-primary). Weekdays and public holidays
need local calendar features, and municipal school holidays use separate files rather than
a national API. These are planned model inputs, not implemented pipeline features.

Identify clients, avoid test loops and preserve raw responses in `landing/<source>/`.
Keep source grain, e.g. weather `(station, time)` or counts `(point, hour)`.
Coordinate mappings from NSR quays and DATEX sites connect enrichment to stop calls.
These enrichment joins must preserve fact counts. See [Silver's join rules](../pipeline/silver.md#rules-in-brief).

## Terms and references

MET uses NLOD + CC BY 4.0, with credit "Based on data from MET Norway".
Vegvesen and Kystverket use NLOD. Name NPRA as the source.

- [Frost how-to](https://frost.met.no/howto.html), [MET licensing](https://www.met.no/en/free-meteorological-data/Licensing-and-crediting)
- [Trafikkdata](https://trafikkdata.atlas.vegvesen.no/)
- [DATEX at Vegvesen](https://www.vegvesen.no/fag/teknologi/apne-data/et-utvalg-apne-data/hva-er-datex)
- [Kystverket AIS](https://www.kystverket.no/en/sea-transport-and-ports/ais/access-to-ais-data/)
- [Google pricing](https://developers.google.com/maps/billing-and-pricing/pricing), [Google service terms §19.3](https://cloud.google.com/maps-platform/terms/maps-service-terms)
