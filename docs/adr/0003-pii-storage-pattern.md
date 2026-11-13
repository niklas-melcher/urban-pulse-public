# ADR 0003: Synthetic PII Storage Pattern (Store Raw, Mask by Default)

**Status:** Accepted
**Date:** 2026-08-03

## Context

Real Telraam/Luchtmeetnet data carries no personal information — bronze
and silver never contain PII, and ADR 0001/0002 don't need to account
for it. This project wants hands-on practice with redaction/masking
policy regardless, so a small, clearly-synthetic "segment device owner"
dataset is introduced (segment_id -> name, email) — not derived from
any real API, purely to exercise the pattern.

Two approaches were considered:
- **Minimize at ingestion**: never store raw PII at all, only ever
  land a pseudonym. Smallest blast radius, but irreversible — no way
  to recover raw values if the pseudonymization scheme ever needs to
  change, or a legitimate reason to see raw data arises.
- **Store raw, mask by default**: raw PII lands once in a restricted
  table; all normal/downstream access goes through a masked view.
  Mirrors how platforms like Databricks Unity Catalog handle row/column
  masking — raw data exists once, access is what's restricted.

## Decision

Use the store-raw-mask-by-default pattern:
- `raw_segment_owners` — restricted table, holds synthetic raw PII
- `segment_owners_masked` — a view exposing only a pseudonym, used by
  all normal pipeline/dashboard code
- A single gated function, `get_raw_owner()`, is the only sanctioned
  path to the raw table, and requires a `reason` argument that gets
  logged — simulating an audit trail a real access-control system
  would enforce structurally.
- Synthetic owner data is joined to real pipeline data (silver_cycling)
  at query time via segment_id, using the masked view by default — the
  same pattern as joining a fact table to a dimension table containing
  sensitive attributes. It is never merged into bronze or silver storage
  directly, since those tables must remain a faithful representation of
  what the source APIs actually returned.

## Consequences

- DuckDB has no real row/column-level security or per-user roles, so
  the "who's allowed to see raw" boundary is enforced by convention
  and code structure (only `get_raw_owner()` touches the raw table),
  not by the database engine itself. This is a simulation of the
  pattern, not a production-grade implementation of it.
- Real Telraam/Luchtmeetnet data (bronze, silver) is untouched by this
  — the synthetic owner data is a fully separate subsystem, matching
  the fact that no real source in this project actually carries PII.