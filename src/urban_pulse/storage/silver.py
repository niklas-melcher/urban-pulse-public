from datetime import datetime
from typing import Any

import duckdb

from urban_pulse.models.air_quality import AirQualityReading
from urban_pulse.models.cycling import CyclingCount
from urban_pulse.storage.hashing import natural_key_hash

CHUNK_SIZE = 500


def land_cycling_to_silver(
    conn: duckdb.DuckDBPyConnection,
    counts: list[CyclingCount],
    fetched_at: datetime,
) -> int:
    """Upsert validated cycling counts in chunks, keyed by
    (segment_id, measured_at). Later records win on duplicate keys."""
    # One row per key per statement: a multi-row ON CONFLICT DO UPDATE
    # can't touch the same row twice.
    items = list({natural_key_hash(c.segment_id, c.measured_at): c for c in counts}.items())

    landed = 0
    for i in range(0, len(items), CHUNK_SIZE):
        chunk = items[i : i + CHUNK_SIZE]
        placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(chunk))
        params: list[Any] = []
        for key, c in chunk:
            params.extend(
                [
                    key,
                    c.segment_id,
                    c.measured_at,
                    c.uptime,
                    c.pedestrian_count,
                    c.bike_count,
                    c.car_count,
                    c.heavy_count,
                    fetched_at,
                ]
            )
        result = conn.execute(
            f"""
            INSERT INTO silver_cycling (
                natural_key, segment_id, measured_at, uptime,
                pedestrian_count, bike_count, car_count, heavy_count, fetched_at
            )
            VALUES {placeholders}
            ON CONFLICT (natural_key) DO UPDATE SET
                uptime = excluded.uptime,
                pedestrian_count = excluded.pedestrian_count,
                bike_count = excluded.bike_count,
                car_count = excluded.car_count,
                heavy_count = excluded.heavy_count,
                fetched_at = excluded.fetched_at
            RETURNING natural_key
            """,
            params,
        ).fetchall()
        landed += len(result)
    return landed


def land_air_quality_to_silver(
    conn: duckdb.DuckDBPyConnection,
    readings: list[AirQualityReading],
    fetched_at: datetime,
) -> int:
    """Upsert validated air quality readings in chunks, keyed by
    (station_number, pollutant, measured_at). Later records win."""
    items = list(
        {
            natural_key_hash(r.station_number, r.pollutant, r.measured_at): r for r in readings
        }.items()
    )

    landed = 0
    for i in range(0, len(items), CHUNK_SIZE):
        chunk = items[i : i + CHUNK_SIZE]
        placeholders = ", ".join(["(?, ?, ?, ?, ?, ?)"] * len(chunk))
        params: list[Any] = []
        for key, r in chunk:
            params.extend([key, r.station_number, r.pollutant, r.value, r.measured_at, fetched_at])
        result = conn.execute(
            f"""
            INSERT INTO silver_air_quality (
                natural_key, station_number, pollutant, value, measured_at, fetched_at
            )
            VALUES {placeholders}
            ON CONFLICT (natural_key) DO UPDATE SET
                value = excluded.value,
                fetched_at = excluded.fetched_at
            RETURNING natural_key
            """,
            params,
        ).fetchall()
        landed += len(result)
    return landed
