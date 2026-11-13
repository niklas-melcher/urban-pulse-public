-- Plain (ungoverned) tags on real tables — descriptive metadata, search/discovery
ALTER TABLE urban_pulse.silver.cycling SET TAGS ('domain' = 'mobility', 'layer' = 'silver');
ALTER TABLE urban_pulse.bronze.cycling_raw SET TAGS ('layer' = 'bronze', 'pii' = 'false');

-- Governed tags — account-level, policy-enforceable. Run once; re-running errors
-- with ALREADY_EXISTS rather than silently no-op'ing (no IF NOT EXISTS clause
-- exists for this statement).
CREATE GOVERNED TAG pii VALUES ('name', 'email');
CREATE GOVERNED TAG sensitivity VALUES ('low', 'high');

-- Demo table — none of our real tables have identity-like columns, so this
-- synthetic table (same spirit as ADR 0003's fake segment owners) is what
-- the policies below actually target.
CREATE TABLE IF NOT EXISTS urban_pulse.bronze.segment_owners_demo (
    segment_id BIGINT,
    owner_name STRING,
    owner_email STRING
);

INSERT INTO urban_pulse.bronze.segment_owners_demo VALUES
    (9000011475, 'Anna de Vries', 'anna@example.com'),
    (9000011585, 'Bram Jansen', 'bram@example.com'),
    (9000004878, 'Sofia Bakker', 'sofia@example.com');

-- Apply governed tags to the columns/table the policies will match on
ALTER TABLE urban_pulse.bronze.segment_owners_demo ALTER COLUMN owner_name SET TAGS ('pii' = 'name');
ALTER TABLE urban_pulse.bronze.segment_owners_demo ALTER COLUMN owner_email SET TAGS ('pii' = 'email');
ALTER TABLE urban_pulse.bronze.segment_owners_demo SET TAGS ('sensitivity' = 'high');

-- Column mask policy: applies to ANY column tagged 'pii' within bronze,
-- present or future — this is the actual ABAC payoff, no per-table SQL
CREATE FUNCTION urban_pulse.bronze.mask_pii(val STRING)
RETURNS STRING
RETURN '***REDACTED***';

CREATE POLICY mask_owner_pii
ON SCHEMA urban_pulse.bronze
COLUMN MASK urban_pulse.bronze.mask_pii
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag('pii') AS col
ON COLUMN col;

-- Row filter policy: applies to ANY table in bronze tagged sensitivity=high
CREATE FUNCTION urban_pulse.bronze.restrict_high_sensitivity()
RETURNS BOOLEAN
RETURN is_account_group_member('admins');

CREATE POLICY restrict_sensitive_rows
ON SCHEMA urban_pulse.bronze
ROW FILTER urban_pulse.bronze.restrict_high_sensitivity
TO `account users`
FOR TABLES
WHEN has_tag_value('sensitivity', 'high');

-- Grants, on real schemas
GRANT USE CATALOG ON CATALOG urban_pulse TO `account users`;
GRANT USE SCHEMA ON SCHEMA urban_pulse.gold TO `account users`;
GRANT SELECT ON SCHEMA urban_pulse.gold TO `account users`;