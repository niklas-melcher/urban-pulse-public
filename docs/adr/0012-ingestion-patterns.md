# ADR 0012: File Ingestion Patterns (Auto Loader, COPY INTO)

**Status:** Accepted
**Date:** 2026-10-05

## Context

Wanted hands-on comparison of Databricks file-ingestion patterns
against one landing volume (urban_pulse.bronze.landing), reusing the
JSON export produced for the Kafka exercise instead of new API calls.

## Decisions

**Auto Loader in directory listing mode, as a streaming table in a
triggered (not continuous) pipeline.** A streaming table processes
only files not yet seen, but a triggered update runs, drains what is
available, and stops. Continuous mode would keep serverless compute
running permanently, which conflicts with the Free Edition fair-use
constraint and with this project's scheduled-job architecture (same
reasoning as trigger(availableNow=True) in ADR 0010).

**COPY INTO via a bundle sql_task, for idempotent triggered batch
loads.** It tracks already-loaded files, so rerunning the same
statement is a safe no-op. The export is a pretty-printed JSON array,
not NDJSON, so FORMAT_OPTIONS ('multiLine' = 'true') is required;
without it every line lands in _corrupt_record.

**File notification mode with file events was attempted and failed.**
cloudFiles.useManagedFileEvents raised
CF_MANAGED_FILE_EVENTS_ENDPOINT_NOT_FOUND ("No location found for uri
/Volumes/..."). File events require an external location with file
events enabled; this volume is managed and has no external location.
Classic file notification (cloudFiles.useNotifications) is documented
as unsupported for Unity Catalog volumes.

## Consequences

- Directory listing is the only discovery mode usable in this
  environment. It scales worse than file events for high file
  volumes, which is immaterial at this project's scale.
- The file-events script is kept in the repo as a documented failed
  experiment; its pipeline resource is commented out of
  databricks.yml so deploys stay clean.
- Revisit on a workspace where external locations and file events
  can be configured.