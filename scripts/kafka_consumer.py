import json

from confluent_kafka import Consumer

TOPIC = "cycling-events"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "urban-pulse-consumer",
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([TOPIC])

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
