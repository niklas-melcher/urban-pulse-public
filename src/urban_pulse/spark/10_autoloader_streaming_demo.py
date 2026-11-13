from typing import Any, cast

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

spark: Any  # injected by the Databricks pipeline runtime; undefined locally


@dp.table(
    name="autoloader_demo_streaming",
    comment="Streaming table via Auto Loader (cloudFiles), directory listing mode",
)
def autoloader_demo_streaming() -> DataFrame:
    return cast(
        DataFrame,
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useNotifications", "false")  # explicit: directory listing mode
        .option("multiLine", "true")  # same file-shape gotcha hit in the COPY INTO exercise
        .load("/Volumes/urban_pulse/bronze/landing/")
        .select("*"),
    )
