# ADR 0001: Bronze vs Silver Deduplication Strategy

**Status:** Accepted
**Date:** 2026-08-02

## Context

Records fetched from Telraam/Luchtmeetnet need deduplication so reruns
don't create duplicate rows. Two dedup strategies were considered:

- **Content hash** (hash of the full record) — treats any change in a
  record's values as a new row.
- **Natural-key hash** (e.g. segment_id + measured_at) — treats records
  with the same identifying fields as the same row, regardless of
  whether values changed.

Sources can retroactively revise past data (e.g. a sensor recalibration
correcting an earlier hour's count).

## Decision

Use both, at different layers:

- **Bronze**: content hash. Bronze is an append-only, immutable log of
  everything ever fetched. A content hash here only prevents re-landing
  an identical fetch twice — a genuine correction produces a new row,
  preserving full history.
- **Silver**: natural-key dedup. For each (segment_id, measured_at) [or
  station_number + pollutant + measured_at for air quality], keep only
  the latest bronze row by fetched_at as the current value.

## Consequences

- Bronze can contain multiple rows for the same real-world hour if a
  source corrects its data — this is intentional, not a bug.
- Silver always presents one row per natural key — safe for downstream
  aggregation without further dedup logic.
- Silver transform logic needs a "pick latest per key" step, not just
  a straight pass-through of bronze.
- The bronze primary key constraint is enforced via `INSERT OR IGNORE`,
  so re-landing an already-seen record is a silent no-op, not an error
  a caller needs to handle.
- `CyclingIngestor.fetch()` uses the watermarks table to narrow its
  request window to "since last successful run" instead of always
  pulling the last 24h, reducing redundant API calls and the bronze
  growth caused by Telraam's data-refinement-over-time behavior (see
  earlier discussion). `AirQualityIngestor` does NOT use the watermark
  the same way — Luchtmeetnet's measurements endpoint has no "since"
  parameter, only pagination from most-recent — so watermarking there
  would only skip *downstream* work (which content-hash dedup already
  handles), not reduce the API call itself. This asymmetry is
  intentional, not an inconsistency.