from datetime import datetime
from typing import Any, cast

import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from tenacity import retry, stop_after_attempt, wait_exponential

dbutils: Any  # injected by the Databricks runtime; undefined in local lint/dev environments

spark = SparkSession.builder.getOrCreate()

station_numbers = dbutils.secrets.get(  # noqa: F821
    scope="urban-pulse", key="luchtmeetnet-station-numbers"
).split(",")

BASE_URL = "https://api.luchtmeetnet.nl/open_api"
TABLE_NAME = "urban_pulse.bronze.air_quality_raw"
WATERMARK_TABLE = "urban_pulse.bronze.watermarks"
MAX_PAGES = 5  # safety cap: chunk through history in bounded steps,
# never fetch unboundedly far back in one run


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
def fetch_station(station_number: str, watermark: datetime | None) -> list[dict[str, Any]]:
    all_records: list[dict[str, Any]] = []
    for page in range(1, MAX_PAGES + 1):
        response = requests.get(
            f"{BASE_URL}/stations/{station_number}/measurements",
            params={"page": page},
            timeout=10.0,
        )
        response.raise_for_status()
        data = cast(list[dict[str, Any]], response.json().get("data", []))
        if not data:
            break
        for record in data:
            record["station_number"] = station_number
        all_records.extend(data)

        if watermark is None:
            break  # first run: page 1 only

        oldest_on_page = min(r["timestamp_measured"] for r in data)
        if oldest_on_page <= watermark.isoformat():
            break  # caught up to the watermark, no need for older pages
    else:
        print(f"WARNING: station {station_number} hit MAX_PAGES without reaching watermark")

    return all_records


watermark = get_watermark("air_quality")

all_reports: list[dict[str, Any]] = []
for station_number in station_numbers:
    all_reports.extend(fetch_station(station_number, watermark))

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

latest = df.agg(F.max(F.to_timestamp("timestamp_measured"))).collect()[0][0]
update_watermark("air_quality", latest)

print(f"landed {landed} new rows to {TABLE_NAME}")
