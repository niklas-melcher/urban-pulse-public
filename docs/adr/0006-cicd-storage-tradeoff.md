# ADR 0006: CI/CD Scheduling Uses GitHub Actions Cache (Interim)

**Status:** Accepted (interim)
**Date:** 2026-08-10

## Context

GitHub-hosted Actions runners are ephemeral — each run starts from a
fresh, empty machine with no disk persisted from prior runs. This
project's core design (bronze content-hash dedup, silver upsert,
watermark-narrowed fetches) depends on data/urban_pulse.duckdb
persisting across runs; without it, every scheduled run would behave
as a first-ever run, defeating the watermark and dedup logic entirely.

Two options considered:
- Use actions/cache to persist data/ between scheduled runs.
- Keep GitHub Actions to lint/test/build only, run actual scheduled
  ingestion on infrastructure with real persistent disk.

## Decision

Use actions/cache for now. This makes "genuinely scheduled, genuinely
stateful" ingestion work end-to-end via GitHub Actions, which is
worth having built and understood at least once.

## Consequences

- This is an explicitly acknowledged compromise, not production-grade
  durability: GitHub's cache can be evicted (unused caches expire,
  and there are size limits), so a cache miss silently resets the
  pipeline to first-run behavior with no error or warning.
- Plan to revisit with real durable storage (e.g. cloud object storage
  for the DuckDB file, or migrating the warehouse) and a real
  orchestrator (e.g. Prefect/Airflow, or Cloud Scheduler + Cloud Run
  on GCP) once the project matures past learning-project scope.

## Update: Schedule Removed (Databricks Now Owns Scheduling)

**Date:** 2026-09-01

The hourly `schedule:` trigger described above has been removed from
`.github/workflows/ci.yml`. Once the Databricks/PySpark extension
took over real ingestion scheduling (ADR 0007), keeping GitHub
Actions' schedule active as well would have meant two independent
schedulers writing to two separate, diverging databases (the
DuckDB/GitHub Actions pipeline and the Databricks/Delta pipeline) —
already a known limitation, not worth compounding with duplicate
scheduled runs.

`workflow_dispatch` (manual trigger) is retained, so the DuckDB
pipeline can still be run on demand if needed. The `quality` job
(lint/type/test) continues running on every push/PR — that part of
ADR 0006 is unaffected.