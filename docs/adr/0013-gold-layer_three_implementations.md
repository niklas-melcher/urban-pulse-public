# ADR 0013: Gold Layer — Three Implementations Compared

**Status:** Accepted (comparison ongoing)
**Date:** 2026-10-27

## Context

The daily gold rollups (cycling per segment; air quality per station
and pollutant) are built on Databricks three ways over the same silver
data, to compare approaches directly rather than assert a preference:
plain PySpark, Lakeflow Declarative Pipelines (SDP), and dbt.

## Implementations

| | Plain PySpark | SDP | dbt |
|---|---|---|---|
| Code | `05_gold.py` | `09_`, `13_` | `dbt/models/gold/` |
| Tables | `*_daily`, `*_latest` | `*_gold_sdp` | `*_gold_dbt` |
| Reads | plain silver | SDP silver via `dp.read()` | plain silver via `source()` |
| Refresh | full overwrite per run | managed MV (cycling showed Incremental) | full table rebuild per run |
| Runs as | job task | pipeline task in the job | manual `dbt run` from a laptop |
| DQ on gold | none | `expect_or_drop`, inline | dbt tests, post-build |

All land in `urban_pulse.gold`. Only the plain version also builds
the `*_latest` snapshots.

## Findings

- **DQ timing differs.** SDP expectations drop violating rows before
  they land and record counts; dbt tests run after the table exists
  and only report. Same inline vs. post-hoc split as ADR 0004. The
  plain script has no gold checks at all.
- **Ordering.** The plain version relies on job `depends_on`; SDP
  declares dependencies in code via `dp.read()` and shows the DAG;
  dbt builds its own DAG from `ref()`/`source()` but is not part of
  the job's DAG.
- **Operations.** dbt is the only variant not scheduled, so its
  freshness depends on manual runs.

Not yet measured: run time and quota use per variant; whether air
quality gold refreshes incrementally; whether a bundle `dbt_task`
works on Free Edition.

## Decision

Keep all three as parallel, independently runnable implementations.
Nothing downstream consumes them yet (the Streamlit dashboard reads
DuckDB). Which one becomes the default for new gold tables is left
open until dbt is scheduled, so the three can be compared under the
same operating conditions.

## Consequences

- Three sets of gold tables to maintain and duplicate computation;
  acceptable at this data volume.
- Revisit when dbt is scheduled, or when a consumer must pick one.
