import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

TOPIC = "taxi-events"

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda data: json.dumps(data).encode("utf-8"),
    retries=5
)

event_id = 1

print("REALTIME TAXI PRODUCER STARTED")
print("Nhan Ctrl+C de dung.")

try:
    while True:
        distance = round(random.uniform(0.5, 15.0), 2)
        fare = round(3.0 + distance * 2.5, 2)
        total = round(fare * 1.08875 + random.uniform(0, 5), 2)

        event = {
            "event_id": event_id,
            "pickup_datetime": datetime.now().isoformat(timespec="seconds"),
            "pickup_zone_id": random.randint(1, 265),
            "trip_distance": distance,
            "fare_amount": fare,
            "total_amount": total
        }

        producer.send(TOPIC, event).get(timeout=10)

        print("SENT:", json.dumps(event, ensure_ascii=False))
        event_id += 1
        time.sleep(1)

except KeyboardInterrupt:
    print("\nDa dung producer.")

finally:
    producer.close()
