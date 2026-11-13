import asyncio
import logging
import os
from collections.abc import Sequence

import httpx

from urban_pulse.ingest.air_quality import AirQualityIngestor
from urban_pulse.ingest.base import IngestResult
from urban_pulse.ingest.cycling import CyclingIngestor
from urban_pulse.models.settings import Settings
from urban_pulse.storage.connection import get_connection
from urban_pulse.storage.gold import create_gold_views
from urban_pulse.storage.retention import purge_old_bronze
from urban_pulse.storage.schema import create_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_step_summary(results: Sequence[IngestResult], purged: int) -> None:
    """If running inside GitHub Actions, write a markdown summary that
    shows up directly on the run page — no log-digging required."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return  # no-op locally, where this env var doesn't exist

    lines = [
        "## Ingestion summary",
        "| Source | Fetched | Landed (bronze) | Landed (silver) |",
        "|---|---|---|---|",
    ]
    for r in results:
        lines.append(
            f"| {r.source} | {r.records_fetched} | "
            f"{r.records_landed_bronze} | {r.records_landed_silver} |"
        )
    lines.append(f"\nPurged **{purged}** bronze rows older than the retention window.")

    with open(summary_path, "a") as f:
        f.write("\n".join(lines) + "\n")


async def run_pipeline() -> None:
    settings = Settings()
    conn = get_connection(settings)
    create_tables(conn)
    create_gold_views(conn)

    async with httpx.AsyncClient() as client:
        cycling = CyclingIngestor(client, conn, settings)
        air_quality = AirQualityIngestor(client, conn, settings)

        # both ingestors' network calls run concurrently — no reason to
        # wait for Telraam to finish before starting Luchtmeetnet
        results = await asyncio.gather(cycling.run(), air_quality.run())

    for result in results:
        logger.info("ingested: %s", result)

    purged = purge_old_bronze(conn)
    logger.info("purged %d bronze rows older than retention window", purged)

    write_step_summary(results, purged)


if __name__ == "__main__":
    asyncio.run(run_pipeline())
