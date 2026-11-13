# src/urban_pulse/quality/silver_checks.py
import duckdb


def run_silver_cycling_checks(conn: duckdb.DuckDBPyConnection) -> bool:
    """Lightweight, hand-rolled data quality checks against silver_cycling.
    Not GX — GX's DuckDB SQLAlchemy integration hit a confirmed library
    bug (IndexError computing table.row_count) during evaluation; see
    ADR 0004."""
    checks = {
        "no null measured_at": "SELECT COUNT(*) FROM silver_cycling WHERE measured_at IS NULL",
        "uptime out of [0,1]": "SELECT COUNT(*) FROM silver_cycling WHERE uptime < 0 OR uptime > 1",
        "negative bike_count": "SELECT COUNT(*) FROM silver_cycling WHERE bike_count < 0",
    }

    all_passed = True
    for description, query in checks.items():
        row = conn.execute(query).fetchone()
        bad_count = row[0] if row is not None else 0
        passed = bad_count == 0
        print(f"{description}: {'PASS' if passed else f'FAIL ({bad_count} rows)'}")
        if not passed:
            all_passed = False

    return all_passed
