# Data Protection Impact Assessment (lite) — urban-pulse

**Date:** 2026-08-03

## What data is collected

- **Cycling/pedestrian counts** (Telraam) — aggregate counts per street
  segment, per hour. No images, no individual detections — Telraam
  devices upload tallies only, by design.
- **Air quality readings** (Luchtmeetnet) — pollutant concentrations
  per station, per hour. Environmental measurements, no personal data.

## Why it's collected

Personal side project practicing data engineering patterns (ingestion,
validation, storage, retention) on real public open data.

## Re-identification risk

Low. Aggregate hourly counts cannot be tied to a specific individual
without external data this project doesn't hold (e.g. CCTV, device-
level GPS). A low-traffic segment showing "1 pedestrian" at a given
hour is a count, not an identity.

## Synthetic PII (separate from the above)

A small synthetic "segment device owner" dataset (name/email) was
added purely to practice redaction/masking patterns — see ADR 0003.
It is not derived from any real API and is stored/access-controlled
separately from real pipeline data (raw_segment_owners table, masked
by default via segment_owners_masked view, raw access gated + logged).

## Retention

See ADR 0002. Bronze: 90 days. Silver: indefinite (bounded by natural
key, not fetch history).

## Mitigations in place

- No raw PII in real pipeline data (bronze/silver for cycling/air
  quality never carry personal fields)
- Synthetic PII stored raw only in a restricted table, masked by
  default for all other access, raw access gated + logged (ADR 0003)
- Bronze pruned after 90 days (ADR 0002, enforced by scheduled purge)

## What would change this assessment

If a future data source added anything genuinely identifying (e.g. a
crowdedness sensor reporting device MAC addresses, or precise GPS
trajectories), this doc and the retention/masking approach would need
to be revisited — the current low-risk conclusion depends on the
sources staying aggregate/anonymous-by-design.
