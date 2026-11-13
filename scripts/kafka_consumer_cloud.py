import json

from confluent_kafka import Consumer

from urban_pulse.models.settings import Settings

settings = Settings()

if not (
    settings.redpanda_bootstrap_servers
    and settings.redpanda_username
    and settings.redpanda_password
    and settings.redpanda_topic
):
    raise RuntimeError("Redpanda Cloud credentials not set in .env")

consumer = Consumer(
    {
        "bootstrap.servers": settings.redpanda_bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": settings.redpanda_username,
        "sasl.password": settings.redpanda_password,
        "group.id": "urban-pulse-consumer",
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([settings.redpanda_topic])

print("listening for messages (Ctrl+C to stop)...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"error: {msg.error()}")
            continue
        payload = json.loads(msg.value())
        print(
            f"segment {payload['segment_id']} @ {payload['measured_at']}: "
            f"{payload['bike_count']} bikes"
        )
finally:
    consumer.close()
