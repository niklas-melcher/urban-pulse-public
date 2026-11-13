# src/urban_pulse/models/air_quality.py
from datetime import datetime

from pydantic import BaseModel, field_validator


class AirQualityReading(BaseModel):
    """One measurement for one pollutant at one station, one hour."""

    station_number: str
    pollutant: str
    value: float
    measured_at: datetime

    @field_validator("measured_at")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("expected a timezone-aware datetime")
        return v
