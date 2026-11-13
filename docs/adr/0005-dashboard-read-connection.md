# ADR 0005: Dashboard Uses a Read-Only DuckDB Connection

**Status:** Accepted
**Date:** 2026-08-05

## Context

DuckDB allows only one read-write connection to a database file at a
time (single-writer, embedded engine — unlike client-server databases
built for concurrent connections). The dashboard originally opened a
normal read-write connection and called create_tables()/create_gold_views()
on every load, which collided with any ingestion script holding the
file open at the same time, causing IOException lock errors.

## Decision

The dashboard opens its DuckDB connection with read_only=True, and
does not create or modify schema. It assumes bronze/silver/gold
already exist, created by ingestion (which owns all writes).

## Consequences

- Dashboard and ingestion can now run concurrently without lock
  conflicts — DuckDB permits multiple read-only connections alongside
  a single writer (or zero writers).
- The dashboard cannot be the first thing run against a fresh database
  — at least one ingestion run (which calls create_tables/create_gold_views)
  must happen first, or the dashboard will fail with a "table does not
  exist" error rather than a lock error.
- Reinforces the need for a single real pipeline entry point (Phase 5)
  that reliably sets up schema — currently this only happens via
  scratch scripts, which is fragile.