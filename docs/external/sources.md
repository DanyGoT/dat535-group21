# External sources (non-Entur)

Data that can explain delays: weather, road traffic and road events. Checked 2026-10-22 with
single test requests and each provider's terms; DATEX measured 2026-10-23/26, including how
well it lines up with real vehicle positions ([datex/content.md](../datex/content.md)).

## Overview

| Provider | Gives us | Access | History | Verdict | Serves ([pipeline/gold.md](../pipeline/gold.md)) |
| -------- | -------- | ------ | ------- | ------- | ------------------ |
| **Statens vegvesen: DATEX II 3.1** | road works, closures, accidents; travel time vs free-flow per road section; road weather incl. surface condition | free account from NPRA (we have one) | **none**: current snapshot only | **collect continuously**: `collector/datex.py`; its weather stations are a median 5.5 km from buses, its situations within 2 km of half the buses | 1, 3, 4, 5 |
| MET Norway: Frost | observed weather per station, incl. coastal stations | free client id (basic-auth user); 401 without | yes | **only for ferries**: DATEX road stations are a median 14 km from ferries and inland; fetch daily if we build the ferry/wind product | 5 |
| Statens vegvesen: Trafikkdata | vehicles per hour per counting point | free, no key; tested OK | yes (~3 h behind) | **skip**: counts cars, not delay; DATEX travel time already measures congestion on the roads it covers | — |
| Kystverket: AIS | ship positions (raw NMEA over TCP) | free, no registration | no | skip: ferry positions already come from Entur's vehicle positions | 5 |
| Google Maps: Routes | travel times with traffic | 5–10k free/month, then paid; terms forbid caching | no | **reject**: can't build a dataset | — |

**Honest scope.** Only DATEX is worth collecting now: it has no history, and it lines up with
where buses drive. Whether road events and weather actually *explain* delay is what Gold #4
and #5 measure; they can come out weak. Frost waits until the ferry product is chosen.

Endpoints:

| Source | Endpoint |
| ------ | -------- |
| DATEX | `https://datex-server-get-v3-1.atlas.vegvesen.no/datexapi/<Service>/pullsnapshotdata` |
| Frost | `https://frost.met.no/observations/v0.jsonld` |
| Trafikkdata | `POST https://trafikkdata-api.atlas.vegvesen.no` (GraphQL), `trafficData { volume { byHour } }` |
| AIS | raw TCP `153.44.253.27:5631` |

## Notes

- **Sources with history need no collector.** Frost can be fetched by a daily batch for
  yesterday and backfilled to the start of our SIRI collection, whenever we decide to.
- **Sources without history need one, now.** DATEX (like SIRI) is lost for every day we don't
  poll. Its road weather overlaps Frost but adds road surface temperature and condition.
- **Forecasts** (for predicting departures days ahead, [pipeline/gold.md](../pipeline/gold.md) #1) are not collected: NPRA's
  road surface forecast (DATEX) or MET's location forecast would be the sources.
- **Calendar** features (public holidays, weekday) are computed locally. School holidays vary
  by municipality and are published as separate files, not a national API.
- **Licences:** MET is NLOD + CC BY 4.0 ("Based on data from MET Norway"). Vegvesen and
  Kystverket are NLOD; NPRA must be named as the source.
- **Politeness:** as with Entur. Identify the client, never loop in tests, store raw
  responses unchanged in `landing/<source>/`.

## Joining to transit

Each source keeps its own grain, e.g. weather `(station, time)` and traffic `(point, hour)`.
It reaches stop calls through a small mapping table, e.g. quay → nearest weather station,
built from coordinates (NSR quays, DATEX site tables). The [pipeline/silver.md](../pipeline/silver.md) rule applies: the join
must not change the row count.

## Links

- [Frost how-to](https://frost.met.no/howto.html), [MET licensing](https://www.met.no/en/free-meteorological-data/Licensing-and-crediting)
- [Trafikkdata](https://trafikkdata.atlas.vegvesen.no/)
- [DATEX at Vegvesen](https://www.vegvesen.no/fag/teknologi/apne-data/et-utvalg-apne-data/hva-er-datex)
- [Kystverket AIS](https://www.kystverket.no/en/sea-transport-and-ports/ais/access-to-ais-data/)
- [Google pricing](https://developers.google.com/maps/billing-and-pricing/pricing), [Google service terms §19.3](https://cloud.google.com/maps-platform/terms/maps-service-terms)
