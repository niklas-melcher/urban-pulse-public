# ADR 0011: Unity Catalog Governance via ABAC, and DABs Schema Policy

**Status:** Accepted
**Date:** 2026-09-23

## Context

Wanted hands-on practice with Unity Catalog governance (tags, row/column
security, grants) and a few DABs features not yet used (sync excludes,
run_as, permissions). Two real design questions came up while scoping
this: how to implement masking/filtering, and how to manage this
config with version control rather than clicking through the UI.

## Decisions

**ABAC over manual per-table functions**: Unity Catalog's
attribute-based access control (row filter and column mask policies,
governed tags) went GA in May 2026. Rather than the originally-planned
pattern of one hand-written masking function + `ALTER TABLE` per table
(the same shape as ADR 0003's synthetic PII pattern), policies are
declared once at the schema level and match any column/table carrying
a given governed tag — including ones that don't exist yet. This is
Databricks' own recommended approach for anything beyond a single
table, and directly exercises tags as load-bearing (policy-driving),
not just descriptive metadata.

**Governance SQL lives in git, applied via a bundle job**: rather than
pasting SQL into the Catalog Explorer UI (no history, no review),
`sql/uc_governance.sql` is version-controlled and run through a
dedicated bundle `sql_task` job (`apply_uc_governance`), not scheduled
— governance changes should be deliberate, not automatic.

**New Unity Catalog schemas go through DABs `resources.schemas`,
existing ones don't get migrated**: `resources.schemas` is a stable
bundle resource with a native `grants:` block. Existing `bronze`/
`silver`/`gold` schemas were created manually via SQL Editor before
this was adopted, and a real, documented failure mode exists for
adding a matching `resources.schemas` block for an object DABs didn't
create — `bundle deploy` treats it as needing deletion and recreation,
risking real data loss. The safe adoption path, `databricks bundle
deployment bind`, exists but is deliberately deferred rather than
risked today.

**Demo table for masking/filtering**: none of urban-pulse's real
tables have identity-like columns to mask, so `segment_owners_demo`
(synthetic, same spirit as ADR 0003) is the actual target of the
column mask and row filter policies — not a real production table.

## Consequences

- As the sole account owner, both the row filter (`admins`-only) and
  column mask (`admins`-only) can't be observed failing/redacting
  from this account alone — the mechanism is real and correctly
  configured, but full end-to-end verification (seeing the masked/
  filtered view as a non-admin) would need a second account.
- `sql/uc_governance.sql` is not fully idempotent — `CREATE GOVERNED
  TAG` has no `IF NOT EXISTS` clause and errors on rerun; acceptable
  for a run-once governance script, worth wrapping if this becomes a
  repeated pattern.
- Terraform (Databricks provider's `PolicyInfo` resource) remains a
  more complete IaC option than the bundle-sql_task approach here —
  deliberately deferred, noted as a future item, not needed to get
  real version-controlled governance today.
