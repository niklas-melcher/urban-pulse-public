# src/urban_pulse/ingest/cycling.py
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import duckdb
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from urban_pulse.ingest.base import Ingestor
from urban_pulse.models.cycling import CyclingCount  # noqa: F401 if unused directly
from urban_pulse.models.settings import Settings
from urban_pulse.storage.silver import land_cycling_to_silver
from urban_pulse.storage.watermarks import get_watermark
from urban_pulse.transform.cycling import parse_cycling_reports

TELRAAM_URL = "https://telraam-api.net/v1/reports/traffic"


class CyclingIngestor(Ingestor[CyclingCount]):
    def __init__(
        self, client: httpx.AsyncClient, conn: duckdb.DuckDBPyConnection, settings: Settings
    ) -> None:
        super().__init__(client, conn)
        self._settings = settings

    @property
    def source_name(self) -> str:
        return "cycling"

    async def fetch(self) -> dict[str, Any]:
        """Fetch traffic counts for all configured segments since the
        last watermark (or the last 24h, on a first run)."""
        segment_ids = [s.strip() for s in self._settings.telraam_segment_ids.split(",")]

        watermark = get_watermark(self._conn, self.source_name)
        now = datetime.now(UTC)
        start = watermark if watermark is not None else now - timedelta(hours=24)

        time_start = start.strftime("%Y-%m-%d %H:%M:%SZ")
        time_end = now.strftime("%Y-%m-%d %H:%M:%SZ")

        results: dict[str, list[dict[str, Any]]] = {}
        for segment_id in segment_ids:
            reports = await self._fetch_segment(segment_id, time_start, time_end)
            results[segment_id] = reports

        return results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def _fetch_segment(
        self, segment_id: str, time_start: str, time_end: str
    ) -> list[dict[str, Any]]:
        response = await self._client.post(
            TELRAAM_URL,
            headers={
                "X-Api-Key": self._settings.telraam_api_key,
                "Content-Type": "application/json",
            },
            json={
                "id": segment_id,
                "time_start": time_start,
                "time_end": time_end,
                "level": "segments",
                "format": "per-hour",
            },
            timeout=10.0,
        )
        response.raise_for_status()
        body = response.json()
        return cast(list[dict[str, Any]], body.get("report", []))

    def parse(self, raw: dict[str, Any]) -> list[CyclingCount]:
        return parse_cycling_reports(raw)

    def _land_silver(self, records: list[CyclingCount], fetched_at: datetime) -> int:
        return land_cycling_to_silver(self._conn, records, fetched_at)
