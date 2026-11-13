# ADR 0004: Data Quality Tooling — Hand-Rolled SQL, Not Great Expectations

**Status:** Accepted
**Date:** 2026-08-03

## Context

Wanted hands-on practice with data quality checks against silver_cycling
(non-null measured_at, uptime within [0,1], non-negative bike_count).
Great Expectations was the initial choice, given its prevalence in the
DE job market, using GX 1.x's ephemeral context + SQLAlchemy DuckDB
connector (duckdb-engine).

## Problem encountered

Every GX expectation failed identically, regardless of column or check
type, with the same root exception:
    `IndexError: list index out of range
    at sqlalchemy_execution_engine.py:1023, computing metric table.row_count`

The failure occurs before GX evaluates the actual expectation logic —
it fails while computing a base metric (row count) needed by every
check. This points to a bug in GX's DuckDB SQLAlchemy execution engine,
not a data or configuration problem on this project's side.

## Decision

Use hand-rolled SQL assertions instead (`src/urban_pulse/quality/silver_checks.py`):
plain `SELECT COUNT(*) ... WHERE <violates constraint>` queries, `PASS`
if count is 0. Same practical outcome (expressing and checking data
quality rules against silver), without depending on a library
integration with a confirmed bug in this exact configuration.

## Consequences

- No GX-specific tooling (data docs, expectation suites, checkpoints)
  in this project's core path — checks are plain functions, easy to
  read, easy to extend, but without GX's richer reporting/ecosystem.
- Revisit GX for the future PySpark/Databricks extension (see memory
  notes) — GX's Spark/Databricks integration is a different code path
  than the DuckDB one that failed here, and Databricks also has its
  own native expectations (Lakeflow/DLT) worth comparing directly.
- If a future `duckdb-engine`/`great-expectations` version resolves
  this bug, revisiting GX for the DuckDB path is a reasonable option,
  not a closed door.

## Update: Native Expectations Comparison (Databricks Extension)

**Date:** 2026-08-27

The Spark/Databricks extension provided the environment originally
intended for revisiting GX — its Spark integration is a different
code path than the DuckDB one that failed (see original decision
above) — and also enabled a direct comparison against Databricks'
own native data quality mechanism, Lakeflow Declarative Pipelines'
`@dlt.expect_or_drop`.

### What was built

`src/urban_pulse/spark/06_cycling_silver_sdp.py` reimplements
`02_cycling_silver.py`'s validation logic (non-null timestamp,
non-negative counts, uptime bounded 0-1) as six named
`@dlt.expect_or_drop` rules, running as a scheduled pipeline
dependent on `fetch_cycling` within `urban_pulse_job`.

### Result

Against 819 real bronze records: 100% written, 0% dropped, all six
expectations passing. Confirms the hand-rolled `.filter(...)` logic
in the plain PySpark silver script and the declarative expectations
produce equivalent validation outcomes on this data.

### Comparison

| | Hand-rolled (`.filter()` in plain scripts) | Native (`@dlt.expect_or_drop`) |
|---|---|---|
| Visibility | `print()` only, no history | Named per-rule pass/fail %, in UI and queryable via the pipeline's event log (a Delta table — `event_log(TABLE(...))`) |
| Enforcement point | After data has already landed in silver | Inline with the transformation itself — violating rows never land |
| Reusability | A standalone function, runnable against any already-landed table regardless of how it got there | Coupled to the specific pipeline that declares it |
| Failure handling options | Binary (pass/fail in your own code) | Three built-in policies: warn (retain + log), drop, or fail the whole pipeline update |

### Decision

No change to the hand-rolled DuckDB approach (ADR 0004 original
decision stands for that path) — this section documents the
Databricks-native alternative now that it's been built and evidenced,
closing the comparison this ADR originally deferred. For future
Spark/Databricks work in this project, `@dlt.expect_or_drop` is the
preferred mechanism over hand-rolled `.filter()` checks, given its
built-in tracking and tighter coupling to the pipeline that needs
the guarantee.