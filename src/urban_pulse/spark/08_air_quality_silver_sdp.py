from typing import Any, cast

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

spark: Any  # injected by the Databricks pipeline runtime; undefined locally


@dp.table(
    name="air_quality_silver_sdp",
    comment="SDP-managed silver, mirrors 04_air_quality_silver.py",
)
@dp.expect_or_drop("value_not_null", "value IS NOT NULL")  # type: ignore[attr-defined, untyped-decorator]
@dp.expect_or_drop("measured_at_not_null", "measured_at IS NOT NULL")  # type: ignore[attr-defined, untyped-decorator]
@dp.expect_or_drop("station_number_not_null", "station_number IS NOT NULL")  # type: ignore[attr-defined, untyped-decorator]
def air_quality_silver_sdp() -> DataFrame:
    return cast(
        DataFrame,
        spark.read.table("urban_pulse.bronze.air_quality_raw").select(  # noqa: F821
            F.col("station_number"),
            F.col("formula").alias("pollutant"),
            F.col("value").cast("double"),
            F.to_timestamp("timestamp_measured").alias("measured_at"),
        ),
    )
