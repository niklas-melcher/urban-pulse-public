-- union() demo: concatenates as-is, duplicates NOT removed (unlike SQL UNION,
-- which dedupes by default — union() behaves like UNION ALL).
-- Each branch wrapped in parens: LIMIT must be unambiguously scoped to one
-- side of the UNION, not the combined result.
(SELECT segment_id, measured_at, bike_count FROM urban_pulse.silver.cycling LIMIT 5)
UNION ALL
(SELECT segment_id, measured_at, bike_count FROM urban_pulse.silver.cycling LIMIT 5);
-- expect 10 rows back, including full duplicates — proves no dedup happened

-- explode() demo: one row PER ARRAY ELEMENT, not "struct column -> array column".
-- posexplode (not plain explode) also returns the array's position, which is
-- the actual useful part for a fixed-order histogram like this one.
SELECT
    segment_id,
    date AS measured_at,
    posexplode(car_speed_hist_0to70plus) AS (bucket_index, vehicle_count)
FROM urban_pulse.bronze.cycling_raw
LIMIT 20;
-- each original row's 8-element speed-histogram array becomes 8 separate rows