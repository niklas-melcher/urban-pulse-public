from datetime import UTC, datetime

import duckdb
import pytest

from urban_pulse.models.cycling import CyclingCount
from urban_pulse.storage import bronze
from urban_pulse.storage.bronze import land_to_bronze
from urban_pulse.storage.schema import create_tables
from urban_pulse.storage.silver import land_cycling_to_silver

NOW = datetime(2026, 10, 12, tzinfo=UTC)


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    create_tables(c)
    return c


def test_bronze_landing_is_idempotent(conn: duckdb.DuckDBPyConnection) -> None:
    records = [{"id": i} for i in range(5)]
    assert land_to_bronze(conn, "t", records, NOW) == 5
    assert land_to_bronze(conn, "t", records, NOW) == 0


def test_bronze_duplicates_within_batch_land_once(conn: duckdb.DuckDBPyConnection) -> None:
    assert land_to_bronze(conn, "t", [{"id": 1}, {"id": 1}, {"id": 2}], NOW) == 2


def test_bronze_chunk_boundaries(
    conn: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bronze, "CHUNK_SIZE", 2)  # 5 records -> chunks of 2, 2, 1
    assert land_to_bronze(conn, "t", [{"id": i} for i in range(5)], NOW) == 5
    row = conn.execute("SELECT COUNT(*) FROM bronze").fetchone()
    assert row is not None and row[0] == 5


def test_silver_duplicate_key_in_batch_last_wins(conn: duckdb.DuckDBPyConnection) -> None:
    base = {
        "segment_id": 1,
        "measured_at": NOW,
        "uptime": 0.9,
        "pedestrian_count": 1.0,
        "car_count": 1.0,
        "heavy_count": 1.0,
    }
    first = CyclingCount(**base, bike_count=10.0)
    second = CyclingCount(**base, bike_count=99.0)
    land_cycling_to_silver(conn, [first, second], NOW)
    rows = conn.execute("SELECT bike_count FROM silver_cycling").fetchall()
    assert rows == [(99.0,)]
