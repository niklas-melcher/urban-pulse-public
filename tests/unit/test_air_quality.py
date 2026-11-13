from datetime import UTC, datetime
from typing import Any

import duckdb
import pytest

from urban_pulse.models.air_quality import AirQualityReading
from urban_pulse.storage import silver
from urban_pulse.storage.schema import create_tables
from urban_pulse.storage.silver import land_air_quality_to_silver
from urban_pulse.transform.air_quality import parse_air_quality_readings

NOW = datetime(2026, 10, 29, tzinfo=UTC)


@pytest.fixture
def conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    create_tables(c)
    return c


def _reading(value: float, hour: int = 12) -> AirQualityReading:
    return AirQualityReading(
        station_number="NL1",
        pollutant="NO2",
        value=value,
        measured_at=datetime(2026, 10, 28, hour, tzinfo=UTC),
    )


def _raw(formula: str, hour: int, value: float | None = None) -> dict[str, Any]:
    record: dict[str, Any] = {
        "formula": formula,
        "timestamp_measured": f"2026-10-28T{hour:02d}:00:00+00:00",
    }
    if value is not None:
        record["value"] = value
    return record


def test_parse_skips_record_missing_value() -> None:
    raw = {"NL1": [_raw("NO2", 12, 14.0), _raw("O3", 13)]}
    result = parse_air_quality_readings(raw)
    assert len(result) == 1
    assert result[0].pollutant == "NO2"


def test_silver_duplicate_key_in_batch_last_wins(conn: duckdb.DuckDBPyConnection) -> None:
    land_air_quality_to_silver(conn, [_reading(10.0), _reading(99.0)], NOW)
    rows = conn.execute("SELECT value FROM silver_air_quality").fetchall()
    assert rows == [(99.0,)]


def test_silver_rerun_updates_in_place(conn: duckdb.DuckDBPyConnection) -> None:
    land_air_quality_to_silver(conn, [_reading(10.0)], NOW)
    land_air_quality_to_silver(conn, [_reading(12.5)], NOW)
    rows = conn.execute("SELECT value FROM silver_air_quality").fetchall()
    assert rows == [(12.5,)]


def test_silver_chunk_boundaries(
    conn: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(silver, "CHUNK_SIZE", 2)  # 5 keys -> chunks of 2, 2, 1
    readings = [_reading(float(h), hour=h) for h in range(5)]
    assert land_air_quality_to_silver(conn, readings, NOW) == 5
    row = conn.execute("SELECT COUNT(*) FROM silver_air_quality").fetchone()
    assert row is not None and row[0] == 5
