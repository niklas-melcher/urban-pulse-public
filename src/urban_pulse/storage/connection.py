from pathlib import Path

import duckdb

from urban_pulse.models.settings import Settings


def get_connection(settings: Settings) -> duckdb.DuckDBPyConnection:
    """Single entry point for getting a DuckDB connection. Ensures the
    parent directory exists first, since DuckDB creates the database
    file itself but not missing directories in its path."""
    db_path = Path(settings.duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))
