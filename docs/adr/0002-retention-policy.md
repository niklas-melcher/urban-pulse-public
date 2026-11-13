# ADR 0002: Data Retention Policy

**Status:** Accepted
**Date:** 2026-08-03

## Context

Bronze grows unboundedly by design (ADR 0001) — every genuinely new
content hash lands a new row, including corrections to past hours.
Left unpruned, this grows indefinitely. Silver holds only the latest
value per natural key, so it doesn't grow the same way.

## Decision

- **Bronze**: retain 90 days. Long enough to audit/debug recent
  ingestion issues or investigate a source's correction behavior;
  short enough to not accumulate indefinitely.
- **Silver**: retain indefinitely. It's bounded by (segment/station ×
  pollutant × hour), not by fetch history, so it doesn't carry the
  same unbounded-growth risk bronze does.

## Consequences

- A purge job is needed, deleting bronze rows older than 90 days
  (by `fetched_at`), run manually for now, later on a schedule
  (Phase 5, CI/CD).
- Once purged, bronze's audit trail for that period is gone — if
  deeper historical audit ever becomes a real requirement, this
  number should be revisited before assuming 90 days is permanent.

## Update: Predictive Optimization Discovered Already Running

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

**Consequence for `07_maintenance.py`**: this script's `OPTIMIZE`/
`VACUUM` calls are likely redundant on Free Edition, duplicating work
Databricks already performs automatically. Kept in the repo as a
learning artifact (demonstrates understanding of the mechanism
explicitly, and would matter on a workspace where Predictive
Optimization is disabled or unavailable), but not required for
correct operation here.

**Also confirmed**: `DESCRIBE HISTORY` fails on `cycling_silver_sdp`
(`EXPECT_TABLE_NOT_VIEW`) — Lakeflow materialized views register in
Unity Catalog as views, not plain tables, with the real Delta storage
managed behind them by the pipeline. `event_log(TABLE(...))` is the
correct diagnostic surface for anything SDP-managed; `DESCRIBE HISTORY`
only works on directly-managed Delta tables like the plain
`spark_python_task` outputs.