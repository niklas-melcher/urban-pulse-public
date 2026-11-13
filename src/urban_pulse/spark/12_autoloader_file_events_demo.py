"""Auto Loader with managed file events (cloudFiles.useManagedFileEvents).

Expected to fail on Free Edition: see ADR 0012.
"""

from typing import Any, cast

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

spark: Any  # injected by the Databricks pipeline runtime; undefined locally


@dp.table(
    name="autoloader_file_events_demo",
    comment="Auto Loader, file notification mode with file events (experiment)",
)
def autoloader_file_events_demo() -> DataFrame:
    return cast(
        DataFrame,
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("multiLine", "true")
        .load("/Volumes/urban_pulse/bronze/landing/"),
    )
