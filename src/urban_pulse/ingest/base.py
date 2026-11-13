# src/urban_pulse/ingest/base.py
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar

import duckdb
import httpx
from pydantic import BaseModel

from urban_pulse.storage.bronze import land_to_bronze
from urban_pulse.storage.watermarks import update_watermark

ModelT = TypeVar("ModelT", bound=BaseModel)


@dataclass(frozen=True)
class IngestResult:
    source: str
    fetched_at: datetime
    records_fetched: int
    records_landed_bronze: int
    records_landed_silver: int


class Ingestor(ABC, Generic[ModelT]):
    """Contract every data source implements: fetch raw data, turn it into
    validated models, land it to bronze. Subclasses implement the two
    source-specific steps; this base class owns the shared orchestration.
    """

    def __init__(self, client: httpx.AsyncClient, conn: duckdb.DuckDBPyConnection) -> None:
        self._client = client
        self._conn = conn

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Short identifier used in logs, table names, and content hashes."""

    @abstractmethod
    async def fetch(self) -> dict[str, Any]:
        """Call the source API, return the raw parsed JSON. No validation,
        no transformation — just get the bytes and parse them."""

    @abstractmethod
    def parse(self, raw: dict[str, Any]) -> list[ModelT]:
        """Turn raw JSON into a list of validated pydantic models.
        Should be a thin wrapper around a pure function in transform/."""

    async def run(self) -> IngestResult:
        """Shared orchestration: fetch -> parse -> land bronze -> land
        silver -> advance watermark. Every subclass gets this for free."""
        raw = await self.fetch()
        fetched_at = datetime.now(UTC)

        records = self.parse(raw)
        if not records:
            return IngestResult(
                source=self.source_name,
                fetched_at=fetched_at,
                records_fetched=0,
                records_landed_bronze=0,
                records_landed_silver=0,
            )

        # bronze needs the raw dicts, not the validated models
        raw_records = [r for reports in raw.values() for r in reports]
        bronze_landed = land_to_bronze(self._conn, self.source_name, raw_records, fetched_at)

        silver_landed = self._land_silver(records, fetched_at)

        latest_measured_at = max(r.measured_at for r in records)  # type: ignore[attr-defined]
        update_watermark(self._conn, self.source_name, latest_measured_at)

        return IngestResult(
            source=self.source_name,
            fetched_at=fetched_at,
            records_fetched=len(records),
            records_landed_bronze=bronze_landed,
            records_landed_silver=silver_landed,
        )

    @abstractmethod
    def _land_silver(self, records: list[ModelT], fetched_at: datetime) -> int:
        """Each source calls its own land_*_to_silver function — this is
        the one piece run() genuinely can't share, since bronze is generic
        but silver tables/columns differ per source."""
