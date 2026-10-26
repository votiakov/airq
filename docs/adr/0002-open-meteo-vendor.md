# ADR 0002: Open-Meteo as the air quality vendor

- Status: accepted
- Date: 2026-10-26

## Context

The brief named the OpenWeatherMap Air Pollution API. A follow-up correction asked for
Open-Meteo instead, because it needs no API key. That keeps "run it with a single
command" true for anyone cloning the repository, with no sign-up and no secret to
inject.

Open-Meteo provides:

- an **Air Quality API** (CAMS European and global models): current values and an
  hourly forecast for PM2.5, PM10, NO₂, O₃, SO₂, CO, dust and ammonia, plus European
  and US AQI;
- a **Geocoding API**: place name to coordinates, with country filtering.

It is free for non-commercial use under fair-use limits (thousands of calls per day,
hundreds per minute), with attribution required (CC BY 4.0). Commercial plans add an
`apikey` parameter and dedicated hosts.

## Decision

- Use the Open-Meteo Air Quality and Geocoding APIs behind an
  `airquality.Provider` interface. `internal/openmeteo` is the only package that knows
  the vendor's URLs, parameters and JSON shapes.
- Expose a vendor-neutral, versioned API:
  - `GET /v1/air-quality/{location}`, mirroring the brief's `GET /weather/{location}`;
  - `GET /v1/air-quality/current`;
  - `GET /v1/air-quality/forecast`.

  Responses use our own domain model (pollutants, AQI with category, resolved
  location), not the vendor's arrays.
- Return Open-Meteo's attribution in every response body.
- Support an optional `UPSTREAM_API_KEY` and configurable base URLs for commercial
  plans and self-hosted instances. The key is treated as a secret even though the
  default setup has none (see [Security](../SECURITY.md)).
- Before writing the adapter, capture real responses (success, validation errors,
  empty geocoding results) and build the adapter tests and the mock vendor from them.

## Alternatives considered

| Option | Why not |
|---|---|
| OpenWeatherMap Air Pollution | Requires an API key: setup friction and a secret in the local stack. Replaced by the correction. |
| OpenAQ v3 | Requires a key; station measurements with gaps rather than a gridded model and forecast |
| IQAir / AirVisual, WAQI | Keys or tokens, restrictive free tiers, licence limits on redistribution |

## Consequences

- No credentials are needed to run, test or demo the service.
- **Fair-use limits are the binding constraint.** Caching (10m current, 30m forecast,
  24h geocoding) keeps vendor calls proportional to distinct locations, not to
  traffic. The outbound quota guard (`UPSTREAM_RATE_LIMIT_RPS`) caps bursts per
  replica. It is not a daily budget, so heavy production use needs a commercial plan.
- Model data is gridded (about 11 km in Europe, about 45 km globally). Coordinates are
  rounded to 2 decimals for caching with no loss of information ([ADR 0003](0003-caching.md)).
- Swapping or adding a vendor means a new `Provider` implementation. The HTTP API,
  caching and resilience layers stay unchanged.
