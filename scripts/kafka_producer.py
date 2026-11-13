import json
import time

from confluent_kafka import Producer
from databricks import sql

from urban_pulse.models.settings import Settings

settings = Settings()
TOPIC = "cycling-events"

connection = sql.connect(
    server_hostname=settings.databricks_host,
    http_path=settings.databricks_http_path,
    access_token=settings.databricks_access_token,
)
cursor = connection.cursor()
cursor.execute("SELECT * FROM urban_pulse.silver.cycling ORDER BY measured_at LIMIT 20")
rows = cursor.fetchall()
cursor.close()
connection.close()

producer = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        print(f"delivered to {msg.topic()} [partition {msg.partition()}]")


for row in rows:
    payload = {
        "segment_id": row.segment_id,
        "measured_at": row.measured_at.isoformat(),
        "bike_count": row.bike_count,
        "pedestrian_count": row.pedestrian_count,
    }
    producer.produce(
        TOPIC,
        key=str(row.segment_id),
        value=json.dumps(payload),
        callback=delivery_report,
    )
    producer.poll(0)  # trigger delivery callbacks
    time.sleep(0.5)  # simulate a live feed, not a burst

producer.flush()
print("done publishing")
