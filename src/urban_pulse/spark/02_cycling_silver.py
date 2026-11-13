from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "urban_pulse.bronze.cycling_raw"
SILVER_TABLE = "urban_pulse.silver.cycling"

bronze_df = spark.table(BRONZE_TABLE)

silver_df = (
    bronze_df.select(
        F.col("segment_id").cast("long"),
        F.to_timestamp("date").alias("measured_at"),
        F.col("uptime").cast("double"),
        F.col("pedestrian").cast("double").alias("pedestrian_count"),
        F.col("bike").cast("double").alias("bike_count"),
        F.col("car").cast("double").alias("car_count"),
        F.col("heavy").cast("double").alias("heavy_count"),
    )
    # equivalent of pydantic's Field(ge=0)/(ge=0, le=1) constraints —
    # rows failing these are silently dropped, not raised as errors,
    # since Spark doesn't have a per-row exception model the way
    # pydantic's constructor does
    .filter(
        F.col("measured_at").isNotNull()
        & F.col("uptime").between(0, 1)
        & (F.col("pedestrian_count") >= 0)
        & (F.col("bike_count") >= 0)
        & (F.col("car_count") >= 0)
        & (F.col("heavy_count") >= 0)
    )
    .withColumn(
        "natural_key",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("segment_id"),
                F.col("measured_at").cast("string"),
            ),
            256,
        ),
    )
)

if not spark.catalog.tableExists(SILVER_TABLE):
    silver_df.write.format("delta").saveAsTable(SILVER_TABLE)
    landed = silver_df.count()
else:
    silver_table = DeltaTable.forName(spark, SILVER_TABLE)
    silver_table.alias("target").merge(
        silver_df.alias("source"),
        "target.natural_key = source.natural_key",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    landed = silver_df.count()

print(f"processed {landed} rows into {SILVER_TABLE}")
