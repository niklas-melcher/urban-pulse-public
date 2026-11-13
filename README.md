# Urban Pulse

**A production-shaped data platform tracking cycling, pedestrian, and air quality patterns across Amsterdam — built on two parallel engines: DuckDB/Python and Databricks/Spark.**

Urban Pulse ingests real, live public data from Amsterdam's Telraam traffic-counter network and the Dutch national Luchtmeetnet air quality network, processes it through a medallion (bronze/silver/gold) architecture, and surfaces it through an interactive dashboard — with the entire pipeline implemented twice: once as a lightweight local stack (DuckDB, plain Python), and once as a cloud-native Spark pipeline (Databricks, Delta Lake, Unity Catalog), so the two approaches can be compared directly on identical data.

---

## What it does

- **Ingests** hourly cycling/pedestrian counts (per street segment) and air quality readings (per pollutant, per station) from two independent public APIs with different authentication schemes, request shapes, and pagination behavior.
- **Validates** every record against strict, typed schemas before it's trusted anywhere downstream — malformed data is rejected and logged, never silently propagated.
- **Deduplicates** intelligently across two distinct strategies: content-hash matching at the raw (bronze) layer to preserve a full audit trail of source corrections, and natural-key upserts at the validated (silver) layer to guarantee one current value per real-world reading — with chunked, multi-row batch inserts rather than per-record round-trips.
- **Tracks state incrementally** via watermarking, so scheduled runs only fetch what's genuinely new — not a fixed lookback window that risks silently losing data on a missed run.
- **Enforces data quality** three ways, across silver and gold: hand-rolled SQL assertions in the DuckDB path, native declarative expectations (`@dp.expect_or_drop`, via `pyspark.pipelines`) in Lakeflow Declarative Pipelines, and dbt tests on the dbt-built gold models — with a documented, evidence-based comparison of the first two.
- **Builds a declarative, dependency-aware pipeline DAG**: Databricks gold tables reference silver via `dp.read(...)`, letting the pipeline engine — not application code — own execution order and incremental recomputation.
- **Builds the gold layer in three styles on Databricks** — plain PySpark, declarative materialized views, and dbt models with dbt tests (currently for the cycling rollup) — so the approaches can be compared directly on the same underlying data.
- **Governs data access natively**: Unity Catalog governed tags drive attribute-based access control (ABAC) — row filters and column masks that apply automatically to any column or table carrying a given tag, declared once and version-controlled, not hand-written per table.
- **Protects privacy by design**: a documented DPIA, an explicit retention policy (logical purge on the DuckDB path; Delta tables rely on Databricks' automatic VACUUM), and a store-raw/mask-by-default pattern for any identity-adjacent data.
- **Runs on a schedule, unattended**, on Databricks Jobs (dependency-aware multi-task DAG, email alerting on failure), with GitHub Actions providing the CI quality gate (lint, type-check, tests) on every push plus an on-demand run of the DuckDB pipeline with cache-based state persistence.
- **Ingests via multiple patterns**: scheduled API polling, Auto Loader (`cloudFiles`) incremental file ingestion as a genuine streaming table, and idempotent triggered batch loads via `COPY INTO`.
- **Visualizes current state** through a Streamlit dashboard reading live from the processed data layer.
- **Streams**, as an exploratory extension, the same data through a Kafka-compatible broker (Redpanda Cloud), including a Spark Structured Streaming consumer; the scheduled consumer is currently paused pending a new broker.

---

## Architecture

```
                    ┌─────────────────────┐         ┌─────────────────────┐
                    │   Telraam API       │         │  Luchtmeetnet API   │
                    │ (cycling/pedestrian)│         │   (air quality)     │
                    └──────────┬──────────┘         └──────────┬──────────┘
                               │                                │
              ┌────────────────┴────────────────────────────────┴────────────────┐
              │                                                                    │
     ┌────────▼─────────┐                                              ┌──────────▼─────────┐
     │   DuckDB engine   │                                              │  Databricks engine  │
     │  (Python/pydantic)│                                              │  (PySpark/Delta)    │
     └────────┬──────────┘                                              └──────────┬──────────┘
              │                                                                     │
     bronze (content-hash dedup, append-only audit trail)             bronze (Delta MERGE, same semantics)
              │                                                                     │
     silver (validated, typed, natural-key upsert)                    silver (typed + native/hand-rolled DQ)
              │                                                                     │
     gold (aggregated views: daily rollups, latest snapshot)           gold (plain PySpark, SDP materialized
              │                                                              views, dbt models)
     ┌────────▼──────────┐                                            ┌────────────▼───────────┐
     │ Streamlit dashboard│                                            │ Databricks SQL / Jobs   │
     └────────────────────┘                                            └─────────────────────────┘
```

Both engines share the same conceptual design (documented in the ADRs below) but are implemented independently, each idiomatic to its own platform — the DuckDB path uses an abstract, generic `Ingestor[ModelT]` base class with pydantic validation; the Spark path uses DataFrame transformations, Delta's native `MERGE INTO`, and declarative pipelines where that model fits better than imperative scripts.

---

## Engineering highlights

| Area | What's demonstrated |
|---|---|
| **Type safety & validation** | Pydantic models for every external data contract, with field-level constraints (range, non-null, timezone-awareness) enforced at construction time |
| **OOP design** | A generic abstract base class (`Ingestor[ModelT]`) sharing orchestration logic across two structurally different data sources, each providing only its own fetch/parse implementation |
| **Async where it earns its keep** | Concurrent API calls via `asyncio.gather`, used deliberately only where genuine I/O-bound concurrency benefit exists — not applied to the Spark path, where Spark's own distributed execution model is the idiomatic mechanism instead |
| **Data engineering fundamentals** | Bronze/silver/gold medallion architecture, content-hash and natural-key deduplication, chunked batch inserts, watermark-based incremental loading, idempotent upserts |
| **Declarative pipelines** | Lakeflow Declarative Pipelines (`pyspark.pipelines`) with `dp.read()`-chained, dependency-aware materialized views and streaming tables — the pipeline engine owns execution order and incremental recomputation, not application code |
| **Transformation tooling** | dbt (dbt-databricks) models and tests on Unity Catalog, built alongside PySpark and declarative-pipeline implementations of the same gold layer (dbt currently covers the cycling rollup) |
| **Data quality** | Three implementations across silver and gold — hand-rolled SQL assertions, native Lakeflow expectations, and dbt tests — with the first two built, run, and compared with real evidence |
| **Data governance** | Unity Catalog governed tags driving ABAC row filter and column mask policies, applied declaratively at the schema level rather than per table; grants and lineage |
| **Ingestion patterns** | Scheduled REST API polling, Auto Loader (`cloudFiles`) streaming file ingestion, and idempotent `COPY INTO` triggered batch loads — each demonstrated against the same underlying data for direct comparison |
| **Privacy & compliance** | A written DPIA, a retention policy (logical purge on the DuckDB path; Delta tables rely on Databricks' automatic VACUUM), and a store-raw/mask-by-default access pattern for identity-adjacent data |
| **CI/CD** | A GitHub Actions quality gate (lint → type-check → test, plus an on-demand pipeline run) and a Databricks Asset Bundle (multi-task, multi-resource-type DAG with dependency-aware orchestration, run-as identity, declared permissions, scheduled, alerting on failure) |
| **Containerization** | Multi-stage Docker builds with separate, minimal images for the pipeline and the dashboard |
| **Testing** | Unit tests covering hashing correctness (including a regression guard for key-order sensitivity), transform validation logic, chunked-insert behavior, and a fully mocked external API integration test |
| **Streaming** | A working Kafka-compatible producer/consumer pair (Redpanda) and a Spark Structured Streaming consumer (currently paused), demonstrating topics, partition keys, consumer groups, and offset management |
| **Documented decision-making** | A dedicated `docs/adr/` directory recording every non-trivial engineering trade-off — including ones that didn't work out, and why |

---

## Tech stack

**Core:** Python, `uv`, `pydantic`, `httpx`, `DuckDB`, `tenacity`, `pytest`, `ruff`, `mypy`, `pre-commit`

**Data platform:** Databricks (Free Edition), PySpark, Delta Lake, Unity Catalog (governed tags, ABAC, lineage), Lakeflow Declarative Pipelines, Databricks Asset Bundles, dbt (`dbt-core`, `dbt-databricks`)

**Orchestration & CI/CD:** GitHub Actions, Databricks Jobs, Docker

**Visualization:** Streamlit, Plotly

**Streaming:** Redpanda (Kafka-API-compatible), `confluent-kafka`, Spark Structured Streaming (consumer paused)

---

## Repository structure

```
src/urban_pulse/
    ingest/       API clients — generic Ingestor base class + per-source implementations
    models/       Pydantic schemas — raw contracts and validated domain models
    transform/    Pure functions — raw → validated, unit-tested in isolation
    storage/      DuckDB connection handling, schema, hashing, bronze/silver/gold logic
    privacy/      Retention and redaction utilities
    dashboard/    Streamlit application
    spark/        Databricks/PySpark pipeline — parallel implementation of the same design,
                   including Lakeflow Declarative Pipeline tables (pyspark.pipelines)
dbt/              dbt project — gold models and tests on Databricks (dbt-databricks)
sql/              Version-controlled SQL — Unity Catalog governance (tags, ABAC policies,
                   grants), COPY INTO and union/explode reference demos
tests/
    unit/         Hashing, transform validation, chunked inserts, mocked API integration tests
docs/
    adr/          Architecture Decision Records — every real trade-off, documented as it happened
scripts/          Exploratory and diagnostic scripts (Kafka producer/consumer, explain-plan
                   demo, ad-hoc queries)
databricks.yml    Databricks Asset Bundle definition (jobs, pipelines, scheduling, sync,
                   run-as identity, permissions)
resources/        Bundle resource definitions (jobs, pipelines, governance)
Dockerfile        Multi-stage build — separate pipeline and dashboard targets
docker-compose.yml
.github/workflows/ci.yml
```

---

## Running it locally

```bash
uv sync
cp .env.example .env   # fill in API keys — see .env.example for what's required
uv run python -m urban_pulse.main
uv run streamlit run src/urban_pulse/dashboard/app.py
```

dbt (needs the Databricks credentials from `.env`):

```bash
cd dbt
uv run --env-file ../.env dbt run --profiles-dir .
uv run --env-file ../.env dbt test --profiles-dir .
```

Docker:

```bash
docker build --target pipeline -t urban-pulse-pipeline .
docker build --target dashboard -t urban-pulse-dashboard .
```

---

## Design decisions

Every non-trivial engineering choice in this project — including the ones that didn't survive contact with reality — is documented in [`docs/adr/`](docs/adr/), covering topics like bronze/silver deduplication strategy, retention and privacy patterns, the trade-offs between hand-rolled and native data quality tooling, the Spark/Databricks implementation's dedup and watermarking design, the Structured Streaming Kafka consumer, file ingestion patterns, and Unity Catalog's governance model.

---

## Data sources

- [Telraam](https://telraam.net) — citizen-hosted traffic sensors reporting hourly cycling, pedestrian, and vehicle counts per street segment
- [Luchtmeetnet](https://www.luchtmeetnet.nl) — the Dutch national air quality monitoring network (RIVM)

---

## Status and known limitations

Feature-complete for its original scope and no longer under active
development. Known limitations are documented in the ADRs:

- Databricks side runs on Free Edition: serverless only, no dev/prod
  separation (ADR 0008), and accounts can lapse after inactivity.
- Air quality catch-up can lose hours after missed runs (ADR 0007).
- The Kafka consumer is paused pending a new broker (ADR 0010).
- DuckDB state on GitHub Actions is an interim cache (ADR 0006).
- No retention delete on Databricks bronze; dbt is not scheduled.

---

## Deploying the Databricks bundle

The bundle takes the workspace from your Databricks CLI profile and the
SQL warehouse from a variable (`databricks warehouses list` shows the id):

```bash
databricks bundle plan -p <profile> --var="warehouse_id=<your-warehouse-id>"
databricks bundle deploy -p <profile> --var="warehouse_id=<your-warehouse-id>"
```

Run `plan` first and check it shows no unexpected deletes.