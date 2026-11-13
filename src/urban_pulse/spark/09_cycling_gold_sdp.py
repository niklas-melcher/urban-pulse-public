from typing import cast

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="urban_pulse.gold.cycling_gold_sdp",
    comment="Daily rollup, materialized view — demonstrates dp.read()",
)
@dp.expect_or_drop("avg_uptime_in_range", "avg_uptime BETWEEN 0 AND 1")  # type: ignore[attr-defined, untyped-decorator]
def cycling_gold_sdp() -> DataFrame:
    return cast(
        DataFrame,
        dp.read("cycling_silver_sdp")  # type: ignore[attr-defined]
        .groupBy("segment_id", F.to_date("measured_at").alias("day"))
        .agg(
            F.avg("uptime").alias("avg_uptime"),
            F.sum("pedestrian_count").alias("total_pedestrians"),
            F.sum("bike_count").alias("total_bikes"),
            F.sum("car_count").alias("total_cars"),
            F.sum("heavy_count").alias("total_heavy"),
        ),
    )
