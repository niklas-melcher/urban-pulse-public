from typing import cast

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="urban_pulse.gold.air_quality_gold_sdp",
    comment="Daily rollup per station and pollutant; dp.read() chains to silver",
)
@dp.expect_or_drop("min_not_above_max", "min_value <= max_value")  # type: ignore[attr-defined, untyped-decorator]
@dp.expect_or_drop("avg_value_not_null", "avg_value IS NOT NULL")  # type: ignore[attr-defined, untyped-decorator]
def air_quality_gold_sdp() -> DataFrame:
    return cast(
        DataFrame,
        dp.read("air_quality_silver_sdp")  # type: ignore[attr-defined]
        .groupBy("station_number", "pollutant", F.to_date("measured_at").alias("day"))
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("reading_count"),
        ),
    )
