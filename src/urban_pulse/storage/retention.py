from datetime import UTC, datetime, timedelta

import duckdb

BRONZE_RETENTION_DAYS = 90


def purge_old_bronze(conn: duckdb.DuckDBPyConnection) -> int:
    """Delete bronze rows older than the retention window (ADR 0002).
    Silver is untouched — it's bounded by natural key, not fetch
    history, and retained indefinitely by design."""
    cutoff = datetime.now(UTC) - timedelta(days=BRONZE_RETENTION_DAYS)
    result = conn.execute(
        "DELETE FROM bronze WHERE fetched_at < ? RETURNING content_hash",
        (cutoff,),
    ).fetchall()
    return len(result)
