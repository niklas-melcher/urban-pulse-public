import json
import time

from confluent_kafka import Producer

from urban_pulse.models.settings import Settings

settings = Settings()

if not (
    settings.redpanda_bootstrap_servers
    and settings.redpanda_username
    and settings.redpanda_password
    and settings.redpanda_topic
):
    raise RuntimeError("Redpanda Cloud credentials not set in .env")

with open("data/cycling_export.json") as f:
    records = json.load(f)

producer = Producer(
    {
        "bootstrap.servers": settings.redpanda_bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": settings.redpanda_username,
        "sasl.password": settings.redpanda_password,
    }
)


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        print(f"delivered to {msg.topic()} [partition {msg.partition()}]")


for record in records:
    producer.produce(
        settings.redpanda_topic,
        key=str(record["segment_id"]),
        value=json.dumps(record),
        callback=delivery_report,
    )
    producer.poll(0)
    time.sleep(0.5)

producer.flush()
print("done publishing")
