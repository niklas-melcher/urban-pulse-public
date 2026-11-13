-- COPY INTO demo: idempotent, triggered batch loading, contrasted with
-- Auto Loader's continuously-running incremental model. Reuses the JSON
-- export already produced for the Kafka exercise (data/cycling_export.json,
-- uploaded to the landing volume below) rather than hitting any API again.
--
-- Run the whole file once to set up and do the first load. The second
-- COPY INTO (same statement, unchanged) demonstrates COPY INTO's built-in
-- already-loaded tracking — rerunning it is a safe no-op by default.
-- The final, force-option block is a deliberate override, included only
-- to prove the default behavior is a real guarantee, not a coincidence —
-- NOT meant to be run as part of normal use of this file.
-- NOTE: the exported file is a pretty-printed JSON array (json.dump with
-- indent=2), not NDJSON — FORMAT_OPTIONS('multiLine'='true') is required,
-- or COPY INTO tries to parse each line independently and fails.

CREATE VOLUME IF NOT EXISTS urban_pulse.bronze.landing;

CREATE TABLE IF NOT EXISTS urban_pulse.bronze.copy_into_demo (
    segment_id BIGINT,
    measured_at STRING,
    bike_count DOUBLE,
    pedestrian_count DOUBLE
);

-- First run: loads every file currently in the volume
COPY INTO urban_pulse.bronze.copy_into_demo
FROM '/Volumes/urban_pulse/bronze/landing/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true');

-- Second run, identical statement: COPY INTO already knows which files
-- it loaded last time, so this is a safe no-op — row count should be
-- unchanged, not doubled.
COPY INTO urban_pulse.bronze.copy_into_demo
FROM '/Volumes/urban_pulse/bronze/landing/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true');

-- Sanity check: should report the same count both times above
SELECT COUNT(*) AS row_count FROM urban_pulse.bronze.copy_into_demo;

-- Deliberate override, run manually and separately if you want to see
-- it — NOT part of the normal demo flow:
-- COPY INTO urban_pulse.bronze.copy_into_demo
-- FROM '/Volumes/urban_pulse/bronze/landing/'
-- FILEFORMAT = JSON
-- COPY_OPTIONS ('force' = 'true');