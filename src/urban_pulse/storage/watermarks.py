from datetime import datetime

import duckdb


def get_watermark(conn: duckdb.DuckDBPyConnection, source: str) -> datetime | None:
    """Return the latest measured_at we've successfully ingested for this
    source, or None if we've never ingested it before (first run)."""
    result = conn.execute(
        "SELECT last_measured_at FROM watermarks WHERE source = ?",
        (source,),
    ).fetchone()
    return result[0] if result else None


def update_watermark(
    conn: duckdb.DuckDBPyConnection, source: str, latest_measured_at: datetime
) -> None:
    """Upsert the watermark for this source. Only moves it forward — if
    latest_measured_at is somehow older than what's stored (e.g. a rerun
    processing the same batch), we don't want to accidentally rewind it."""
    conn.execute(
        """
        INSERT INTO watermarks (source, last_measured_at)
        VALUES (?, ?)
        ON CONFLICT (source) DO UPDATE SET
            last_measured_at = GREATEST(watermarks.last_measured_at, excluded.last_measured_at)
        """,
        (source, latest_measured_at),
    )
