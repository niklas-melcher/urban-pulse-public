# src/urban_pulse/ingest/air_quality.py
from datetime import datetime
from typing import Any, cast

import duckdb
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from urban_pulse.ingest.base import Ingestor
from urban_pulse.models.air_quality import AirQualityReading
from urban_pulse.models.settings import Settings
from urban_pulse.storage.silver import land_air_quality_to_silver
from urban_pulse.transform.air_quality import parse_air_quality_readings

LUCHTMEETNET_BASE_URL = "https://api.luchtmeetnet.nl/open_api"


class AirQualityIngestor(Ingestor[AirQualityReading]):
    def __init__(
        self, client: httpx.AsyncClient, conn: duckdb.DuckDBPyConnection, settings: Settings
    ) -> None:
        super().__init__(client, conn)
        self._settings = settings

    @property
    def source_name(self) -> str:
        return "air_quality"

    async def fetch(self) -> dict[str, Any]:
        """Fetch the most recent page of measurements for each configured
        station — all reported pollutants, unfiltered."""
        station_numbers = [
            s.strip() for s in self._settings.luchtmeetnet_station_numbers.split(",")
        ]

        results: dict[str, list[dict[str, Any]]] = {}
        for station_number in station_numbers:
            reports = await self._fetch_station(station_number)
            results[station_number] = reports

        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def _fetch_station(self, station_number: str) -> list[dict[str, Any]]:
        response = await self._client.get(
            f"{LUCHTMEETNET_BASE_URL}/stations/{station_number}/measurements",
            params={"page": 1},
            timeout=10.0,
        )
        response.raise_for_status()
        body = response.json()
        return cast(list[dict[str, Any]], body.get("data", []))

    def parse(self, raw: dict[str, Any]) -> list[AirQualityReading]:
        return parse_air_quality_readings(raw)

    def _land_silver(self, records: list[AirQualityReading], fetched_at: datetime) -> int:
        return land_air_quality_to_silver(self._conn, records, fetched_at)
