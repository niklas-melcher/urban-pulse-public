from typing import Any, cast

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

spark: Any  # injected by the Databricks pipeline runtime; undefined locally


@dp.table(name="cycling_silver_sdp", comment="SDP-managed silver, mirrors 02_cycling_silver.py")
@dp.expect_or_drop("valid_uptime", "uptime BETWEEN 0 AND 1")  # type: ignore[attr-defined, untyped-decorator]
@dp.expect_or_drop("non_negative_pedestrian_count", "pedestrian_count >= 0")  # type: ignore[attr-defined, untyped-decorator]
@dp.expect_or_drop("non_negative_bike_count", "bike_count >= 0")  # type: ignore[attr-defined, untyped-decorator]
@dp.expect_or_drop("non_negative_car_count", "car_count >= 0")  # type: ignore[attr-defined, untyped-decorator]
@dp.expect_or_drop("non_negative_heavy_count", "heavy_count >= 0")  # type: ignore[attr-defined, untyped-decorator]
@dp.expect_or_drop("measured_at_not_null", "measured_at IS NOT NULL")  # type: ignore[attr-defined, untyped-decorator]
def cycling_silver_sdp() -> DataFrame:
    return cast(
        DataFrame,
        spark.read.table("urban_pulse.bronze.cycling_raw").select(  # noqa: F821
            F.col("segment_id").cast("long"),
            F.to_timestamp("date").alias("measured_at"),
            F.col("uptime").cast("double"),
            F.col("pedestrian").cast("double").alias("pedestrian_count"),
            F.col("bike").cast("double").alias("bike_count"),
            F.col("car").cast("double").alias("car_count"),
            F.col("heavy").cast("double").alias("heavy_count"),
        ),
    )
