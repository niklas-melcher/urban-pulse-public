# scripts/try_parse_air_quality.py
"""Throwaway script to check parse_air_quality_readings() against
hand-built fake data — including one deliberately bad record — without
needing to hit the real API. Run with: uv run python scripts/try_parse_air_quality.py
"""

from urban_pulse.transform.air_quality import parse_air_quality_readings

fake_raw = {
    "NL49017": [
        {
            "formula": "NO2",
            "value": 14.0,
            "timestamp_measured": "2026-06-25T14:00:00+00:00",
        },
        {
            "formula": "PM10",
            "value": 22.5,
            "timestamp_measured": "2026-06-25T15:00:00+00:00",
        },
        {
            # deliberately broken: missing "value" entirely
            "formula": "O3",
            "timestamp_measured": "2026-06-25T16:00:00+00:00",
        },
        {
            # deliberately broken: not a real pollutant, if you kept the Enum
            "formula": "NOT_A_REAL_GAS",
            "value": 5.0,
            "timestamp_measured": "2026-06-25T17:00:00+00:00",
        },
    ],
}

readings = parse_air_quality_readings(fake_raw)

print(f"parsed {len(readings)} out of 4 fake records")
for r in readings:
    print(r)
