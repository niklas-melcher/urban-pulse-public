from datetime import UTC, datetime, timedelta
from typing import Any, cast

import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from tenacity import retry, stop_after_attempt, wait_exponential

dbutils: Any  # injected by the Databricks runtime; undefined in local lint/dev environments

spark = SparkSession.builder.getOrCreate()

telraam_api_key = dbutils.secrets.get(scope="urban-pulse", key="telraam-api-key")  # noqa: F821
telraam_segment_ids = dbutils.secrets.get(  # noqa: F821
    scope="urban-pulse", key="telraam-segment-ids"
).split(",")

TELRAAM_URL = "https://telraam-api.net/v1/reports/traffic"
TABLE_NAME = "urban_pulse.bronze.cycling_raw"
WATERMARK_TABLE = "urban_pulse.bronze.watermarks"


def get_watermark(source: str) -> datetime | None:
    if not spark.catalog.tableExists(WATERMARK_TABLE):
        return None
    row = (
        spark.table(WATERMARK_TABLE)
        .filter(F.col("source") == source)
        .select("last_measured_at")
        .collect()
    )
    return row[0]["last_measured_at"] if row else None


def update_watermark(source: str, latest_measured_at: datetime | None) -> None:
    if latest_measured_at is None:
        return
    update_df = spark.createDataFrame(
        [(source, latest_measured_at)], "source string, last_measured_at timestamp"
    )
    if not spark.catalog.tableExists(WATERMARK_TABLE):
        update_df.write.format("delta").saveAsTable(WATERMARK_TABLE)
    else:
        wm_table = DeltaTable.forName(spark, WATERMARK_TABLE)
        wm_table.alias("target").merge(
            update_df.alias("source_df"), "target.source = source_df.source"
        ).whenMatchedUpdate(
            set={
                "last_measured_at": F.greatest(
                    "target.last_measured_at", "source_df.last_measured_at"
                )
            }
        ).whenNotMatchedInsertAll().execute()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def fetch_segment(segment_id: str, time_start: str, time_end: str) -> list[dict[str, Any]]:
    response = requests.post(
        TELRAAM_URL,
        headers={"X-Api-Key": telraam_api_key, "Content-Type": "application/json"},
        json={
            "id": segment_id,
            "time_start": time_start,
            "time_end": time_end,
            "level": "segments",
            "format": "per-hour",
        },
        timeout=10.0,
    )
    response.raise_for_status()
    return cast(list[dict[str, Any]], response.json().get("report", []))


watermark = get_watermark("cycling")
now = datetime.now(UTC)
start = watermark if watermark is not None else now - timedelta(hours=24)

time_start = start.strftime("%Y-%m-%d %H:%M:%SZ")
time_end = now.strftime("%Y-%m-%d %H:%M:%SZ")

all_reports: list[dict[str, Any]] = []
for segment_id in telraam_segment_ids:
    all_reports.extend(fetch_segment(segment_id, time_start, time_end))

print(f"fetched {len(all_reports)} raw reports")

df = spark.createDataFrame(all_reports)
df = df.withColumn("content_hash", F.sha2(F.to_json(F.struct(*df.columns)), 256))

if not spark.catalog.tableExists(TABLE_NAME):
    df.write.format("delta").saveAsTable(TABLE_NAME)
    landed = df.count()
else:
    bronze_table = DeltaTable.forName(spark, TABLE_NAME)
    before = spark.table(TABLE_NAME).count()
    bronze_table.alias("target").merge(
        df.alias("source"), "target.content_hash = source.content_hash"
    ).whenNotMatchedInsertAll().execute()
    after = spark.table(TABLE_NAME).count()
    landed = after - before

latest = df.agg(F.max(F.to_timestamp("date"))).collect()[0][0]
update_watermark("cycling", latest)

print(f"landed {landed} new rows to {TABLE_NAME}")
