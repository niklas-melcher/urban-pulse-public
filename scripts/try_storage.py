# scripts/try_storage.py
import asyncio
from datetime import UTC, datetime

import httpx

from urban_pulse.ingest.cycling import CyclingIngestor
from urban_pulse.models.settings import Settings
from urban_pulse.storage.bronze import land_to_bronze
from urban_pulse.storage.connection import get_connection
from urban_pulse.storage.gold import create_gold_views
from urban_pulse.storage.hashing import content_hash
from urban_pulse.storage.schema import create_tables
from urban_pulse.storage.silver import land_cycling_to_silver

settings = Settings()
conn = get_connection(settings)
create_tables(conn)
create_gold_views(conn)  # idempotent, safe to call multiple times

# TODO: build a small fake record, compute its content_hash,
# INSERT it into bronze, then SELECT it back out and print it to confirm.
# Try inserting the SAME record twice — what happens the second time?

sample_record = {
    "segment_id": 123,
    "measured_at": "2024-06-01T12:00:00Z",
    "uptime": 99.9,
    "pedestrian_count": 42,
    "bike_count": 10,
    "car_count": 100,
    "heavy_count": 5,
}

hash_value = content_hash(sample_record)
print(f"Content hash of sample record: {hash_value}")

conn.execute(
    "INSERT OR IGNORE INTO bronze (content_hash, source, fetched_at, raw_json) VALUES (?, ?, ?, ?)",
    (hash_value, "test_source", "2024-06-01T12:00:00Z", str(sample_record)),
)

count = conn.execute("SELECT COUNT(*) FROM bronze").fetchone()
print(count)

records = [sample_record, sample_record, {**sample_record, "segment_id": 456}]  # 2 dupes + 1 new
landed = land_to_bronze(conn, "test_source", records, datetime.now(UTC))
print(f"landed: {landed}")  # should print 1, not 3 — since two of the three are identical


async def main() -> None:
    settings = Settings()
    conn = get_connection(settings)
    create_tables(conn)
    create_gold_views(conn)

    async with httpx.AsyncClient() as client:
        ingestor = CyclingIngestor(client, conn, settings)
        raw = await ingestor.fetch()
        counts = ingestor.parse(raw)

    print(f"got {len(counts)} validated counts to land")

    landed = land_cycling_to_silver(conn, counts, datetime.now(UTC))
    print(f"landed {landed} cycling rows to silver")

    landed_again = land_cycling_to_silver(conn, counts, datetime.now(UTC))
    print(f"landed {landed_again} on rerun")

    row_count = conn.execute("SELECT COUNT(*) FROM silver_cycling").fetchone()
    print(f"total rows in silver_cycling: {row_count}")


if __name__ == "__main__":
    asyncio.run(main())
