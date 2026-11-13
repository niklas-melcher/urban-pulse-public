-- Time Travel reference queries. Delta retains prior table versions;
-- Predictive Optimization's default 7-day file retention means anything
-- older than that will raise
-- DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION.
-- See ADR 0007's "Predictive Optimization Discovered Already Running" update.

-- See every version of a table, with the operation and timestamp that
-- produced it (MERGE, CREATE TABLE AS SELECT, etc.)
DESCRIBE HISTORY urban_pulse.silver.cycling;

-- Query the table as it existed at a specific version number
-- (pick a real version from the DESCRIBE HISTORY output above)
SELECT * FROM urban_pulse.silver.cycling VERSION AS OF 3;

-- Query the table as it existed at a specific point in time
-- (must be within the last ~7 days, per the retention note above)
SELECT * FROM urban_pulse.silver.cycling TIMESTAMP AS OF '2026-09-20T00:00:00Z';

-- Practical use: compare current vs. a past version for one segment,
-- to see what a specific MERGE actually changed
SELECT *
FROM urban_pulse.silver.cycling
WHERE segment_id = 9000011475
EXCEPT
SELECT *
FROM urban_pulse.silver.cycling VERSION AS OF 3
WHERE segment_id = 9000011475;
