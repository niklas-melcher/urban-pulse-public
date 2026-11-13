import duckdb
import httpx
import pytest
import respx

from urban_pulse.ingest.cycling import CyclingIngestor
from urban_pulse.models.settings import Settings
from urban_pulse.storage.schema import create_tables


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
@respx.mock
async def test_fetch_calls_telraam_and_returns_raw_reports() -> None:
    settings = Settings(
        telraam_api_key="fake-key",
        telraam_segment_ids="123",
        luchtmeetnet_station_numbers="NL00000",
    )
    conn = duckdb.connect(":memory:")
    create_tables(conn)

    respx.post("https://telraam-api.net/v1/reports/traffic").mock(
        return_value=httpx.Response(
            200,
            json={
                "report": [
                    {
                        "segment_id": 123,
                        "date": "2026-08-01T12:00:00.000Z",
                        "uptime": 0.99,
                        "pedestrian": 50.0,
                        "bike": 20.0,
                        "car": 100.0,
                        "heavy": 5.0,
                    }
                ]
            },
        )
    )

    async with httpx.AsyncClient() as client:
        ingestor = CyclingIngestor(client, conn, settings)
        raw = await ingestor.fetch()

    assert "123" in raw
    assert len(raw["123"]) == 1
    assert raw["123"][0]["pedestrian"] == 50.0
