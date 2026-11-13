import logging
from typing import Any

from urban_pulse.models.air_quality import AirQualityReading

logger = logging.getLogger(__name__)


def parse_air_quality_readings(raw: dict[str, list[dict[str, Any]]]) -> list[AirQualityReading]:
    """Pure function: raw Luchtmeetnet payload (keyed by station number) -> validated
    models. Skips and logs individual bad records instead of failing the
    whole batch."""
    readings: list[AirQualityReading] = []
    for station_number, reports in raw.items():
        for report in reports:
            try:
                readings.append(
                    AirQualityReading(
                        station_number=station_number,
                        pollutant=report["formula"],
                        value=report["value"],
                        measured_at=report["timestamp_measured"],
                    )
                )
            except (KeyError, ValueError) as e:
                logger.warning("Skipping malformed report for station %s: %s", station_number, e)
    return readings
