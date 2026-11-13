from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

silver = spark.table("urban_pulse.silver.cycling")

# 1) A groupBy aggregation: rows sharing a key must end up on the same
#    partition, so look for aggregate nodes with a shuffle exchange between.
daily = silver.groupBy("segment_id", F.to_date("measured_at").alias("day")).agg(
    F.avg("uptime").alias("avg_uptime"),
    F.sum("bike_count").alias("total_bikes"),
)
print("=== Physical plan: daily aggregation ===")
daily.explain(mode="formatted")

# 2) A join against a tiny table: look for the join strategy chosen
#    (broadcast vs. shuffle) and whether an exchange appears on each side.
latest = spark.table("urban_pulse.gold.cycling_latest").select(
    "segment_id", F.col("measured_at").alias("latest_measured_at")
)
joined = silver.join(latest, "segment_id")
print("=== Physical plan: join against a ~3-row table ===")
joined.explain(mode="formatted")
