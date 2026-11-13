# scripts/try_privacy.py
"""Throwaway script to exercise the synthetic PII redaction pattern —
seed fake owner data, then compare masked vs. raw access paths.
Run with: uv run python scripts/try_privacy.py
"""

import logging

from urban_pulse.models.settings import Settings
from urban_pulse.privacy.redaction import get_masked_owner, get_raw_owner, seed_fake_owners
from urban_pulse.storage.connection import get_connection
from urban_pulse.storage.gold import create_gold_views
from urban_pulse.storage.schema import create_tables

# so the logger.warning(...) inside get_raw_owner actually prints somewhere
logging.basicConfig(level=logging.INFO)

SEGMENT_ID = 9000011475  # one of the synthetic owners' segment ids

settings = Settings()
conn = get_connection(settings)
create_tables(conn)
create_gold_views(conn)  # idempotent, safe to call multiple times

seed_fake_owners(conn)

print("\n--- normal, sanctioned access path ---")
masked = get_masked_owner(conn, SEGMENT_ID)
print(f"masked owner for segment {SEGMENT_ID}: {masked}")

print("\n--- raw access path (should log a warning) ---")
raw = get_raw_owner(conn, SEGMENT_ID, reason="testing the redaction pattern")
print(f"raw owner for segment {SEGMENT_ID}: {raw}")

print("\n--- a segment with no owner record at all ---")
none_case = get_masked_owner(conn, 999999)
print(f"masked owner for unknown segment: {none_case}")

print("\n--- joining real silver_cycling data to masked owner info ---")
rows = conn.execute(
    """
    SELECT
        sc.segment_id,
        sc.measured_at,
        sc.bike_count,
        som.owner_pseudonym
    FROM silver_cycling sc
    LEFT JOIN segment_owners_masked som ON sc.segment_id = som.segment_id
    ORDER BY sc.measured_at DESC
    LIMIT 10
    """
).fetchall()

for row in rows:
    segment_id, measured_at, bike_count, owner_pseudonym = row
    print(
        f"segment={segment_id}  measured_at={measured_at}  bikes={bike_count}  "
        f"owner={owner_pseudonym}"
    )
