# ADR 0003: In-process caching with stale-if-error and request coalescing

- Status: accepted
- Date: 2026-10-26

## Context

Air quality data changes slowly (CAMS updates hourly), is identical for every client,
and the vendor has fair-use limits. Caching is the main lever for:
- latency;
- vendor load;
- surviving vendor outages.

Requirements:

- bounded memory whatever the input;
- correct, observable freshness;
- the ability to serve recent data when the vendor fails;
- no thundering herd when a popular entry expires.

## Decision

- **Three in-process caches** (current, forecast, geocoding), each an LRU bounded by
  `CACHE_MAX_ENTRIES` (`hashicorp/golang-lru` `simplelru` under our mutex).
- **TTL per data type:**

  | Data | TTL |
  |---|---|
  | Current | 10m |
  | Forecast | 30m |
  | Geocoding | 24h |
  | Geocoding misses (negative cache) | 10m |

- **Stale-if-error:** expired entries stay for `CACHE_STALE_FOR` (1h). They are served
  only when a refresh fails for a vendor-side reason (timeout, 5xx, 429, circuit open,
  throttled), never for our own errors. The response is marked `X-Cache: STALE`,
  `Cache-Status: …; detail=stale-if-error` and `Cache-Control: no-cache`.
- **Request coalescing** with `singleflight`: concurrent misses for the same key share
  one vendor call. The shared call runs detached from any one caller's cancellation,
  with its own timeout, so one impatient caller cannot fail the others.
- **Key normalisation:** coordinates are rounded to 2 decimals (about 1.1 km), finer
  than the model grid, so nearby requests share entries without losing information.
  The forecast always fetches the full 96 hours and slices per request, so `hours`
  does not fragment the cache.
- **HTTP caching semantics** let clients and any CDN cache too:
  - `Cache-Control: public, max-age=<remaining TTL>`;
  - `Age`;
  - a strong `ETag` with `304` on `If-None-Match`.

## Alternatives considered

| Option | Why not (now) |
|---|---|
| Redis or Valkey as a shared cache | Another stateful dependency to run, secure and monitor, plus a network hop and a new failure mode on every request. It pays off when many replicas make per-replica hit ratios poor, or when a global vendor budget is needed. |
| A reverse proxy cache (Varnish, nginx, CDN) only | Cannot implement "serve stale only for vendor failures" with our domain-aware error classification, or coalesce per normalised key. It complements the in-process cache (our headers support it) rather than replacing it. |
| Expiring caches with a single TTL (golang-lru `expirable`, ristretto, otter) | No per-entry stale window (see [ADR 0001](0001-dependency-policy.md)) |
| Proactive refresh-ahead | Extra vendor load for entries nobody may ask for again; stale-if-error covers the availability case |

## Consequences

- Hit, miss and stale ratios are visible per cache (`airq_cache_lookups_total`), along
  with coalescing, evictions and entry counts. The Grafana dashboard plots the hit ratio.
- Each replica warms its own cache. After a deploy, vendor load rises briefly, bounded
  by the quota guard.
- Stale data can be up to `TTL + CACHE_STALE_FOR` old (70 minutes for current
  conditions). Clients can tell from `X-Cache` and `Age`.
- Memory per cache is bounded at `CACHE_MAX_ENTRIES` entries of a few KB each.
