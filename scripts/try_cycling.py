# scripts/try_cycling.py
import asyncio

import httpx

from urban_pulse.ingest.cycling import CyclingIngestor
from urban_pulse.models.settings import Settings
from urban_pulse.storage.connection import get_connection
from urban_pulse.storage.gold import create_gold_views
from urban_pulse.storage.schema import create_tables


async def main() -> None:
    settings = Settings()
    conn = get_connection(settings)
    create_tables(conn)
    create_gold_views(conn)

    async with httpx.AsyncClient() as client:
        ingestor = CyclingIngestor(client, conn, settings)
        result = await ingestor.run()

    print(result)


if __name__ == "__main__":
    asyncio.run(main())
