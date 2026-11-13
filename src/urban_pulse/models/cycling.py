from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class CyclingCount(BaseModel):
    """One hourly count report for one Telraam segment. This is the
    'silver' layer model — validated and narrowed to what this project
    actually uses, not a 1:1 mirror of the API response."""

    segment_id: int
    measured_at: datetime  # UTC, despite the API's separate 'timezone' field
    uptime: float = Field(ge=0, le=1)

    pedestrian_count: float = Field(ge=0)
    bike_count: float = Field(ge=0)
    car_count: float = Field(ge=0)
    heavy_count: float = Field(ge=0)

    @field_validator("measured_at")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("expected a timezone-aware datetime")
        return v
