import os
import json
import csv
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC           = os.getenv("KAFKA_TOPIC",    "raw-mock")
for attempt in range(1, 11):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5
        )
        print(f"Connected to Kafka broker at {KAFKA_BOOTSTRAP}")
        break
    except NoBrokersAvailable:
        print(f"[{attempt}/10] Kafka broker is not ready")
        time.sleep(5)
else:
    raise RuntimeError(f"Unable to connect to Kafka broker at {KAFKA_BOOTSTRAP}")

data_dir = "/data"
for fname in os.listdir(data_dir):
    if not fname.endswith(".csv"):
        continue
    path = os.path.join(data_dir, fname)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(TOPIC, row)

producer.flush()
producer.close()
print(f"Finished sending messages to topic '{TOPIC}'")
