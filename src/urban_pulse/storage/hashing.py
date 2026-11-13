import hashlib
import json
from typing import Any


def content_hash(record: dict[str, Any]) -> str:
    """Bronze-layer hash: the full record's content, canonicalized so
    identical data always hashes the same regardless of key order."""
    canonical = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def natural_key_hash(*parts: Any) -> str:
    """Silver-layer hash: identifying fields only (e.g. segment_id +
    measured_at), used to find/replace the current version of a
    real-world record regardless of whether its values changed."""
    canonical = json.dumps([str(p) for p in parts], sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()
