# ADR 0014: Wind-Down — Pausing the Databricks Pipeline

**Status:** Accepted
**Date:** 2026-11-09

## Context

urban-pulse was a practice project; the author now has a full
Databricks workspace. The scheduled Free Edition pipeline
(urban-pulse-pipeline, daily 03:00 UTC) also stopped firing
repeatedly, with no error and no failure email.

## Observations

- Scheduled runs missing Oct 10-13, Oct 20-22 and from Nov 2 (still
  stopped Nov 9). The Oct 22 gap ended after a manual run.
- Streaks of daily runs between stops: 10, 6 and about 10.
- On Nov 9 the job reported pause_status UNPAUSED while no run had
  fired since Nov 1, so a platform-side pause is ruled out. No run
  was stuck active.
- A "five quiet days after a deploy" hypothesis was rejected: runs
  continued Oct 24-28 after the Oct 23 deploy.
- Free Edition quota shutdowns are documented but unconfirmed.
  Root cause: unknown.

## Consequences observed

- Air quality (page-based API) lost hours in gaps longer than the
  page cap covers; see the correction in ADR 0007. Cycling recovers
  because its fetch uses a time range.
- on_failure alerts cannot detect a run that never starts. A
  heartbeat check was designed but never built.

## Decision

Pause the schedule and keep the repository as a reference
implementation. The freshness detector, air quality backfill and
watermark redesign remain documented future work.

- 2026-11-13: while pausing, a mis-nested `include:` in databricks.yml
  (under `sync:` instead of top level) made the bundle load zero
  resources, and a deploy deleted all jobs and pipelines. They were
  recreated from the corrected files; the old run history was lost.
  Lesson: run `databricks bundle plan` before every deploy.