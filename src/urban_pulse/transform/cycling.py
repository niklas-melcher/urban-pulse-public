import logging
from typing import Any

from urban_pulse.models.cycling import CyclingCount

logger = logging.getLogger(__name__)


def parse_cycling_reports(raw: dict[str, list[dict[str, Any]]]) -> list[CyclingCount]:
    """Pure function: raw Telraam payload (keyed by segment id) -> validated
    models. Skips and logs individual bad records instead of failing the
    whole batch — one malformed hour shouldn't block the other 22."""
    counts: list[CyclingCount] = []
    for segment_id, reports in raw.items():
        for report in reports:
            try:
                counts.append(
                    CyclingCount(
                        segment_id=report["segment_id"],
                        measured_at=report["date"],
                        uptime=report["uptime"],
                        pedestrian_count=report["pedestrian"],
                        bike_count=report["bike"],
                        car_count=report["car"],
                        heavy_count=report["heavy"],
                    )
                )
            except (KeyError, ValueError) as e:
                logger.warning("Skipping malformed report for segment %s: %s", segment_id, e)
    return counts
