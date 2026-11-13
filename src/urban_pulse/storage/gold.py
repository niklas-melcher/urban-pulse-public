import duckdb

GOLD_CYCLING_DAILY_DDL = """
CREATE OR REPLACE VIEW gold_cycling_daily AS
SELECT
    segment_id,
    CAST(measured_at AS DATE) AS day,
    AVG(uptime) AS avg_uptime,
    SUM(pedestrian_count) AS total_pedestrians,
    SUM(bike_count) AS total_bikes,
    SUM(car_count) AS total_cars,
    SUM(heavy_count) AS total_heavy
FROM silver_cycling
GROUP BY segment_id, CAST(measured_at AS DATE)
"""

GOLD_AIR_QUALITY_DAILY_DDL = """
CREATE OR REPLACE VIEW gold_air_quality_daily AS
SELECT
    station_number,
    pollutant,
    CAST(measured_at AS DATE) AS day,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value
FROM silver_air_quality
GROUP BY station_number, pollutant, CAST(measured_at AS DATE)
"""

GOLD_CYCLING_LATEST_DDL = """
CREATE OR REPLACE VIEW gold_cycling_latest AS
SELECT segment_id, measured_at, uptime, pedestrian_count, bike_count, car_count, heavy_count
FROM silver_cycling
QUALIFY ROW_NUMBER() OVER (PARTITION BY segment_id ORDER BY measured_at DESC) = 1
"""

GOLD_AIR_QUALITY_LATEST_DDL = """
CREATE OR REPLACE VIEW gold_air_quality_latest AS
SELECT station_number, pollutant, measured_at, value
FROM silver_air_quality
QUALIFY ROW_NUMBER() OVER (PARTITION BY station_number, pollutant ORDER BY measured_at DESC) = 1
"""


def create_gold_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Idempotent — safe to call on every startup. Views are recreated
    (CREATE OR REPLACE) rather than checked for existence, since a view
    definition might legitimately change as the project evolves."""
    for ddl in (
        GOLD_CYCLING_DAILY_DDL,
        GOLD_AIR_QUALITY_DAILY_DDL,
        GOLD_CYCLING_LATEST_DDL,
        GOLD_AIR_QUALITY_LATEST_DDL,
    ):
        conn.execute(ddl)
