from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

dbutils: Any  # injected by the Databricks runtime; undefined in local lint/dev environments

spark = SparkSession.builder.getOrCreate()

bootstrap = dbutils.secrets.get(scope="urban-pulse", key="redpanda-bootstrap-servers")  # noqa: F821
username = dbutils.secrets.get(scope="urban-pulse", key="redpanda-username")  # noqa: F821
password = dbutils.secrets.get(scope="urban-pulse", key="redpanda-password")  # noqa: F821
topic = dbutils.secrets.get(scope="urban-pulse", key="redpanda-topic")  # noqa: F821

MESSAGE_SCHEMA = StructType(
    [
        StructField("segment_id", LongType()),
        StructField("measured_at", StringType()),
        StructField("bike_count", DoubleType()),
        StructField("pedestrian_count", DoubleType()),
    ]
)

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
    f'username="{username}" password="{password}";'
)

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = raw_stream.select(
    F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("data"),
    F.col("timestamp").alias("kafka_timestamp"),
).select("data.*", "kafka_timestamp")

query = (
    parsed.writeStream.format("delta")
    .option("checkpointLocation", "/Volumes/urban_pulse/bronze/checkpoints/cycling_stream")
    .trigger(availableNow=True)
    .toTable("urban_pulse.bronze.cycling_stream")
)

query.awaitTermination()
print("stream batch complete")
