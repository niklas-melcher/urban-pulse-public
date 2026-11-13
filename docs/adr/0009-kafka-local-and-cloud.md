# ADR 0009: Local Kafka/Redpanda for Learning, Despite Data Circularity

**Status:** Accepted
**Date:** 2026-09-01

## Context

Wanted hands-on Kafka practice (producer/consumer, topics, partitions,
consumer groups, offsets) as a planned extension. The project's real
data already lives in Databricks (silver.cycling), having been
ingested there directly from Telraam/Luchtmeetnet — so replaying that
same data through a local Kafka broker and back doesn't add any new
real-world data flow. It's circular: Databricks -> local Kafka ->
(nowhere useful yet).

## Decision

Build the local Kafka exercise anyway: a script pulls existing
silver.cycling rows from Databricks via the SQL connector
(scripts/try_databricks_pull.py), a producer (scripts/kafka_producer.py)
publishes them to a local Redpanda topic, a consumer
(scripts/kafka_consumer.py) reads them back. Local Redpanda chosen
over a cloud broker specifically because Databricks' cloud compute
cannot reach localhost — connecting Kafka to Databricks for real
requires a cloud-hosted broker (Redpanda Cloud/Confluent Cloud),
deliberately deferred to avoid starting a trial-credit clock before
fundamentals are practiced (see prior memory/planning notes).

Explicitly declined a tunneled (ngrok) connection from Databricks
back to this local broker purely to "read messages in Spark" today —
the mechanics of `spark.readStream.format("kafka")` are identical
whether the broker is tunneled-local or genuinely cloud-hosted, so a
temporary, unauthenticated public tunnel added real fragility and a
security surface for no additional learning value over waiting for
the already-planned cloud broker step.

## Consequences

- This exercise proves Kafka fundamentals (topics, keys/partitioning,
  consumer groups, offsets) in isolation — genuinely useful, but not
  yet integrated into the project's real data flow.
- The circularity is intentional and acceptable: the goal was
  learning the mechanism, not building a production data path.
- Spark/Databricks consuming from Kafka remains a real next step,
  to be done once a cloud-hosted broker is set up — at that point,
  the natural extension is making cycling ingestion genuinely
  event-driven (Telraam polling -> Kafka -> Spark), not just replaying
  already-ingested data for practice.

## Update: Cloud Broker Added (Redpanda Cloud)

**Date:** 2026-09-08

Added a second, parallel set of scripts targeting Redpanda Cloud
(serverless, free trial) rather than local Redpanda:
`export_cycling_from_databricks.py` (pulls silver.cycling via
databricks-sql-connector, writes to a local JSON file),
`kafka_producer_cloud.py` (publishes that file's records, SASL_SSL
authenticated), `kafka_consumer_cloud.py` (same, consumer side).
Local Redpanda scripts (kafka_producer.py, kafka_consumer.py) are
kept as-is, unrenamed — both approaches now exist side by side.

Required two separate ACL grants on the service account: one on the
Topic resource (produce/consume), and a second, easy-to-miss one on
the Consumer Group resource (`urban-pulse-consumer`) — Kafka treats
topic access and consumer-group coordination as genuinely separate
permissions. Missing the second produced
`GROUP_AUTHORIZATION_FAILED`, not a topic-access error, which was
initially confusing.

This unblocks the original goal: Databricks can now reach this
broker directly (unlike local Redpanda), enabling a real
`spark.readStream.format("kafka")` consumer — the next step.

## Update: Databricks Consumer Built

**Date:** 2026-09-15

The Databricks-side consumption this ADR deferred is now built — see
ADR 0010 for the Structured Streaming implementation and the
Kafka-client gotchas encountered (shaded class names, trigger mode).
