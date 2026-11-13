# ADR 0010: Databricks Structured Streaming Kafka Consumer

**Status:** Accepted
**Date:** 2026-09-15

## Context

With a working Kafka pipeline (ADR 0009 — local and cloud producer/
consumer), the remaining piece was having Databricks consume
directly from the cloud broker (Redpanda Cloud), via Spark
Structured Streaming — the actual reason the cloud broker was set up
in the first place.

## Decisions

**Trigger mode**: `trigger(availableNow=True)`, not the default
continuous micro-batch mode or `trigger(continuous)`. Structured
Streaming normally runs as a long-lived process, which doesn't fit
this project's existing job architecture — scheduled, bounded tasks
that run and exit (Databricks Jobs, GitHub Actions). `availableNow`
processes everything currently on the topic, then stops — a
deliberate hybrid, keeping streaming's incremental/fault-tolerant
processing model (checkpointing, exactly-once semantics) while still
behaving as a normal schedulable batch task.

**Checkpointing**: a Unity Catalog Volume
(`urban_pulse.bronze.checkpoints`), not raw DBFS — Volumes are UC's
current managed abstraction for file storage, consistent with using
UC-managed tables for everything else in this project.

## Consequences / gotchas encountered

**Shaded Kafka class prefix required**: the stream failed with
`No LoginModule found for org.apache.kafka.common.security.scram.ScramLoginModule`.
Databricks' Spark runtime bundles its own renamed ("shaded") internal
copy of the Kafka client, under `kafkashaded.org.apache.kafka...`, to
avoid version conflicts on the cluster. The JAAS config string must
reference the shaded class
(`kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule`)
— not the plain `org.apache.kafka...` name the standalone
`confluent-kafka` Python client (used in the local/cloud producer and
consumer scripts) expects. Same credentials, same broker — only the
class prefix differs between Spark's connector and the standalone
Python client.

## Update: `dlt` → `pyspark.pipelines` (`dp`)

**Date:** 2026-10-01

Databricks replaced the `dlt` Python module with `pyspark.pipelines`
(conventionally imported as `dp`) as the current API for Lakeflow
Declarative Pipelines. `dlt` still works but is now legacy. All
pipeline scripts (06, 08) updated to `from pyspark import pipelines as dp`
and `@dp.table`/`@dp.expect_or_drop`.

## Update: Consumer Paused

**Date:** 2026-10-15

`kafka_stream_cycling` was removed from the scheduled job after it
started failing nightly (most likely the Redpanda Cloud trial
ending). The script (`07_kafka_stream_bronze.py`) stays in the repo.
Revisit with a different broker or approach; ADR 0009 describes the
broker setup this depended on.