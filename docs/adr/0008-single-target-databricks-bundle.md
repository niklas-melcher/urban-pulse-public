# ADR 0008: Single Bundle Target, No Dev/Prod Catalog Separation

**Status:** Accepted
**Date:** 2026-08-25

## Context

Attempted to mirror the ingestion pipeline's dev/prod environment
separation on the Databricks side: a `dev` target writing to a
separate `urban_pulse_dev` catalog, and a `prod` target (scheduled,
unpaused) writing to the real `urban_pulse` catalog — the catalog
name passed to each `spark_python_task` script via a bundle variable.

## Problem encountered

No mechanism for passing the catalog name into the scripts at runtime
actually worked, despite trying three different, each independently
documented, approaches:

1. `dbutils.widgets.get(...)` — documented for pipeline/notebook
   parameters, not populated for `spark_python_task`.
2. `named_parameters` on the task — documented, but only for
   `python_wheel_task`, not `spark_python_task`; DAB validation
   rejected it outright.
3. Plain `parameters: ["--catalog", "${var.catalog}"]` read via
   `sys.argv` — the documented mechanism for `spark_python_task`
   specifically, confirmed via Databricks' own bundle examples.
   Empirically, `sys.argv` inside the running script contained only
   the script's own path, no parameters, on every attempt.

Root cause not conclusively identified — most likely a Free Edition /
serverless-compute-specific gap between documented and actual
behavior for `spark_python_task` parameter passing, rather than a
configuration mistake (the final YAML matched Databricks' own
documented examples exactly).

## Decision

Revert to a single bundle target (`dev`, used as the only target,
name is now just a label). Catalog name is hardcoded directly in each
script (`urban_pulse.bronze.cycling_raw`, etc.) rather than
parameterized. The job's schedule is unconditionally unpaused on this
single target.

## Consequences

- No environment separation between "testing" and "real" runs on the
  Databricks side — a manual `databricks bundle run` and the real
  scheduled run both operate on the same `urban_pulse` catalog. This
  is a real, accepted trade-off versus the safer dev/prod split the
  DuckDB/GitHub Actions pipeline has.
- The unused `urban_pulse_dev` catalog/schemas created during this
  attempt can be dropped or left dormant — not part of the design.
- Worth revisiting if Databricks documentation/behavior around
  `spark_python_task` parameters changes, or if this ever moves off
  Free Edition to a standard workspace with classic compute, where
  the parameter-passing mechanisms may behave as documented.