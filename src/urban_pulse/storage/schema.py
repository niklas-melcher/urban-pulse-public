import duckdb

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS bronze (
    content_hash VARCHAR PRIMARY KEY,
    source VARCHAR NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    raw_json VARCHAR NOT NULL
)
"""

SILVER_CYCLING_DDL = """
CREATE TABLE IF NOT EXISTS silver_cycling (
    natural_key VARCHAR PRIMARY KEY,
    segment_id BIGINT NOT NULL,
    measured_at TIMESTAMP NOT NULL,
    uptime DOUBLE NOT NULL,
    pedestrian_count DOUBLE NOT NULL,
    bike_count DOUBLE NOT NULL,
    car_count DOUBLE NOT NULL,
    heavy_count DOUBLE NOT NULL,
    fetched_at TIMESTAMP NOT NULL
)
"""

SILVER_AIR_QUALITY_DDL = """
CREATE TABLE IF NOT EXISTS silver_air_quality (
    natural_key VARCHAR PRIMARY KEY,
    station_number VARCHAR NOT NULL,
    pollutant VARCHAR NOT NULL,
    value DOUBLE NOT NULL,
    measured_at TIMESTAMP NOT NULL,
    fetched_at TIMESTAMP NOT NULL
)
"""

WATERMARKS_DDL = """
CREATE TABLE IF NOT EXISTS watermarks (
    source VARCHAR PRIMARY KEY,
    last_measured_at TIMESTAMP NOT NULL
)
"""


RAW_SEGMENT_OWNERS_DDL = """
CREATE TABLE IF NOT EXISTS raw_segment_owners (
    segment_id BIGINT PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR NOT NULL
)
"""

SEGMENT_OWNERS_MASKED_VIEW = """
CREATE OR REPLACE VIEW segment_owners_masked AS
SELECT
    segment_id,
    substr(sha256(email), 1, 16) AS owner_pseudonym
FROM raw_segment_owners
"""


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for ddl in (
        BRONZE_DDL,
        SILVER_CYCLING_DDL,
        SILVER_AIR_QUALITY_DDL,
        WATERMARKS_DDL,
        RAW_SEGMENT_OWNERS_DDL,
    ):
        conn.execute(ddl)
    conn.execute(SEGMENT_OWNERS_MASKED_VIEW)  # views created after their source table exists
