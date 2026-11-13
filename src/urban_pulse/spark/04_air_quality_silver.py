from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "urban_pulse.bronze.air_quality_raw"
SILVER_TABLE = "urban_pulse.silver.air_quality"

bronze_df = spark.table(BRONZE_TABLE)

silver_df = (
    bronze_df.select(
        F.col("station_number"),
        F.col("formula").alias("pollutant"),  # stays a plain string, per your earlier call
        F.col("value").cast("double"),
        F.to_timestamp("timestamp_measured").alias("measured_at"),
    )
    .filter(F.col("measured_at").isNotNull() & F.col("value").isNotNull())
    .withColumn(
        "natural_key",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("station_number"),
                F.col("pollutant"),
                F.col("measured_at").cast("string"),
            ),
            256,
        ),
    )
)

if not spark.catalog.tableExists(SILVER_TABLE):
    silver_df.write.format("delta").saveAsTable(SILVER_TABLE)
else:
    silver_table = DeltaTable.forName(spark, SILVER_TABLE)
    silver_table.alias("target").merge(
        silver_df.alias("source"), "target.natural_key = source.natural_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

print(f"processed {silver_df.count()} rows into {SILVER_TABLE}")
