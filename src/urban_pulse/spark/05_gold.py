from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# --- cycling daily rollup ---
cycling_silver = spark.table("urban_pulse.silver.cycling")

cycling_daily = cycling_silver.groupBy("segment_id", F.to_date("measured_at").alias("day")).agg(
    F.avg("uptime").alias("avg_uptime"),
    F.sum("pedestrian_count").alias("total_pedestrians"),
    F.sum("bike_count").alias("total_bikes"),
    F.sum("car_count").alias("total_cars"),
    F.sum("heavy_count").alias("total_heavy"),
)

cycling_daily.write.format("delta").mode("overwrite").saveAsTable("urban_pulse.gold.cycling_daily")

# --- cycling latest snapshot ---

latest_window = Window.partitionBy("segment_id").orderBy(F.col("measured_at").desc())

cycling_latest = (
    cycling_silver.withColumn("rn", F.row_number().over(latest_window))
    .filter(F.col("rn") == 1)
    .drop("rn", "natural_key")
)

cycling_latest.write.format("delta").mode("overwrite").saveAsTable(
    "urban_pulse.gold.cycling_latest"
)

# --- air quality daily rollup ---
air_quality_silver = spark.table("urban_pulse.silver.air_quality")

air_quality_daily = air_quality_silver.groupBy(
    "station_number", "pollutant", F.to_date("measured_at").alias("day")
).agg(
    F.avg("value").alias("avg_value"),
    F.min("value").alias("min_value"),
    F.max("value").alias("max_value"),
)

air_quality_daily.write.format("delta").mode("overwrite").saveAsTable(
    "urban_pulse.gold.air_quality_daily"
)

# --- air quality latest snapshot ---
aq_latest_window = Window.partitionBy("station_number", "pollutant").orderBy(
    F.col("measured_at").desc()
)

air_quality_latest = (
    air_quality_silver.withColumn("rn", F.row_number().over(aq_latest_window))
    .filter(F.col("rn") == 1)
    .drop("rn", "natural_key")
)

air_quality_latest.write.format("delta").mode("overwrite").saveAsTable(
    "urban_pulse.gold.air_quality_latest"
)

print("gold tables refreshed")
