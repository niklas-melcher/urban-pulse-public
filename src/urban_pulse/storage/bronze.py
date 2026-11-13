import json
from datetime import datetime
from typing import Any

import duckdb

from urban_pulse.storage.hashing import content_hash

CHUNK_SIZE = 500


def land_to_bronze(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    records: list[dict[str, Any]],
    fetched_at: datetime,
) -> int:
    """Insert raw records into bronze in chunks, deduplicated by content
    hash. Returns the number of rows actually newly landed."""
    # Identical records share a hash: keep one per hash within the batch.
    items = list({content_hash(r): r for r in records}.items())

    landed = 0
    for i in range(0, len(items), CHUNK_SIZE):
        chunk = items[i : i + CHUNK_SIZE]
        placeholders = ", ".join(["(?, ?, ?, ?)"] * len(chunk))
        params: list[Any] = []
        for h, record in chunk:
            params.extend([h, source, fetched_at, json.dumps(record, default=str)])
        result = conn.execute(
            f"""
            INSERT OR IGNORE INTO bronze (content_hash, source, fetched_at, raw_json)
            VALUES {placeholders}
            RETURNING content_hash
            """,
            params,
        ).fetchall()
        landed += len(result)
    return landed
