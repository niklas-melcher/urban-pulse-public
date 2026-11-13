# ADR 0007: Spark Ingestion — Dedup, Watermarking, and Scheduling

**Status:** Accepted
**Date:** 2026-08-21

## Context

Mirroring the DuckDB pipeline's bronze/silver design (ADR 0001) and
watermarking (added post-launch to CyclingIngestor.fetch()) in
PySpark/Databricks, running as scheduled Job tasks against Unity
Catalog Delta tables rather than notebooks.

## Decisions

**Dedup mechanism**: Delta's native `MERGE INTO` replaces DuckDB's
`INSERT OR IGNORE` (bronze, content-hash keyed) and
`INSERT ... ON CONFLICT DO UPDATE` (silver, natural-key keyed).
`whenNotMatchedInsertAll()` mirrors bronze's skip-on-duplicate
behavior; `whenMatchedUpdateAll().whenNotMatchedInsertAll()` mirrors
silver's upsert. Hashes are computed via `sha2(to_json(struct(*cols)))`
— a Spark-native equivalent of the Python `content_hash`/
`natural_key_hash` functions, computed as a DataFrame column
expression rather than per-row in a Python loop, since this needs to
scale across executors, not run in a single process.

**Watermarking**: a Delta `watermarks` table (source, last_measured_at)
mirrors the DuckDB version, including the same `GREATEST()` guard
against regression. Cycling narrows its fetch window using the
watermark directly (Telraam supports a time range parameter). Air
quality has no such parameter — Luchtmeetnet is page-based, most-recent
first — so watermarking there means paginating backward until the
watermark is reached, bounded by a `MAX_PAGES` safety cap (currently 5)
to prevent an unbounded backfill in one run if a gap is unusually
large. If `MAX_PAGES` is exhausted without reaching the watermark, a
warning is printed rather than failing the run — the gap isn't fully
closed, but ingestion still succeeds with partial data, and the next
run will retry the same catch-up (not compound the miss, since the
watermark only advances up to what's actually been landed).

**Scheduling**: daily (was hourly during initial build/testing),
mainly to stay within Free Edition's fair-use compute quota. Daily
cadence is only safe to rely on because of the watermarking above —
without it, any missed/delayed run would silently and permanently
lose data older than a fixed lookback window.

**Retry**: `tenacity` wraps each source's fetch function, mirroring
the original DuckDB ingestors' retry-on-429 behavior after both
sources hit real rate limits during initial testing. For air quality
specifically, the decorator wraps the whole paginated fetch (not a
single page), so a failure partway through a multi-page catch-up
re-fetches from page 1 on retry — wasteful but harmless, since bronze
dedup silently absorbs the re-fetched duplicate pages.

## Consequences

- Known gap vs. the DuckDB/pydantic version: silver's `.filter(...)`
  validation step silently drops rows that fail constraints (null
  timestamps, out-of-range values), with no per-row logging of *why*
  — unlike the Python version's explicit
  `logger.warning("Skipping malformed report...")`. Spark's DataFrame
  API has no natural per-row try/except equivalent; a "rejected rows"
  side-output would be needed to restore that visibility, not built yet.
- `dbutils` (Databricks' injected secrets/notebook API) is a genuine
  runtime global with no local definition, requiring both a bare type
  annotation (`dbutils: Any`) for mypy and `# noqa: F821` at each call
  site for ruff — two different tools, two different reasons the name
  looks "undefined" locally despite being real at runtime.

## Update: Predictive Optimization Discovered Already Running, SDP Incremental Behavior Confirmed

**Date:** 2026-08-27

While investigating a Time Travel query that failed
(`DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION`,
168 hours), discovered Databricks' **Predictive Optimization** — enabled
by default on all Unity Catalog managed tables since 2025 — had already
been running `OPTIMIZE` and `VACUUM` automatically on every table in
this project, with zero manual action taken. This explains the Time
Travel failure: VACUUM's default 7-day file retention had already
pruned versions older than that window before any manual VACUUM was
ever run.

**Consequence**: a `07_maintenance.py` script (manual `OPTIMIZE`/
`VACUUM` on a weekly schedule) was built, found to be redundant given
the above, and removed from the repo rather than kept as dead code.

**Also confirmed**: `DESCRIBE HISTORY` fails on `cycling_silver_sdp`
(`EXPECT_TABLE_NOT_VIEW`) — Lakeflow materialized views register in
Unity Catalog as views, not plain tables, with the real Delta storage
managed behind them by the pipeline. `event_log(TABLE(...))` is the
correct diagnostic surface for anything SDP-managed.

**Resolved — SDP incremental behavior**: queried `cycling_silver_sdp`'s
event log directly (`event_type = 'planning_information'`). First run
was `COMPLETE_RECOMPUTE` (expected — no existing data). Subsequent
runs were `APPEND_ONLY` — confirming genuine incremental processing
is occurring, despite six `@dlt.expect_or_drop` rules being defined.
This contradicts a secondhand doc claim found earlier that expectations
disqualify materialized views from incremental refresh — the empirical
event log evidence is trusted over that claim.

## Correction (2026-10-27): air quality catch-up is not retried

The Watermarking section says that when MAX_PAGES is exhausted
without reaching the watermark, the next run retries the same
catch-up. That is wrong. The watermark is set to the newest timestamp
landed, so readings older than the oldest fetched page are never
revisited and stay missing. The daily-cadence safety argument holds
for cycling (time-range fetch) but not for air quality after a gap
longer than the page cap covers. Fix deferred: advance the watermark
only to the contiguous frontier.