# src/urban_pulse/privacy/redaction.py
import logging

import duckdb

logger = logging.getLogger(__name__)

# Synthetic data — NOT from any real API. Simulates a "device owner"
# record a real Telraam-like deployment might plausibly hold, purely
# to give this project a realistic reason to practice redaction policy.
FAKE_SEGMENT_OWNERS = {
    9000011475: {"name": "Anna de Vries", "email": "anna.devries@example.com"},
    9000011585: {"name": "Bram Jansen", "email": "bram.jansen@example.com"},
    9000004878: {"name": "Sofia Bakker", "email": "sofia.bakker@example.com"},
}


def seed_fake_owners(conn: duckdb.DuckDBPyConnection) -> None:
    """One-time seed of synthetic owner data — not part of the real
    ingestion pipeline, just sets up data to practice redaction on."""
    for segment_id, owner in FAKE_SEGMENT_OWNERS.items():
        conn.execute(
            """
            INSERT INTO raw_segment_owners (segment_id, name, email)
            VALUES (?, ?, ?)
            ON CONFLICT (segment_id) DO UPDATE SET
                name = excluded.name, email = excluded.email
            """,
            (segment_id, owner["name"], owner["email"]),
        )


def get_masked_owner(conn: duckdb.DuckDBPyConnection, segment_id: int) -> str | None:
    """The normal, sanctioned way to reference a segment's owner —
    returns only a pseudonym, never raw PII."""
    result = conn.execute(
        "SELECT owner_pseudonym FROM segment_owners_masked WHERE segment_id = ?",
        (segment_id,),
    ).fetchone()
    return result[0] if result else None


def get_raw_owner(
    conn: duckdb.DuckDBPyConnection, segment_id: int, reason: str
) -> tuple[str, str] | None:
    """The ONLY sanctioned path to raw PII. Every call is logged with
    its stated reason — simulating the audit trail a real access
    control system would enforce structurally, not just by convention."""
    logger.warning("RAW PII ACCESS: segment_id=%s reason=%r", segment_id, reason)
    result = conn.execute(
        "SELECT name, email FROM raw_segment_owners WHERE segment_id = ?",
        (segment_id,),
    ).fetchone()
    return (result[0], result[1]) if result else None
