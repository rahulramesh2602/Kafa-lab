import json
import time
from typing import Dict
from confluent_kafka import Producer

BROKER = "localhost:9092"
TOPIC = "vehicle-status"

def delivery_report(err, msg):
    if err is not None:
        print(f" Delivery failed: {err}")
    else:
        print(f" Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def make_payload(vehicle_id: int, status: str, version: str) -> Dict:
    return {
        "vehicle_id": vehicle_id,
        "status": status,
        "firmware_version": version,
        "ts": int(time.time())
    }

def main():
    p = Producer({"bootstrap.servers": BROKER})
    print(f"Producer connected to {BROKER}, sending to topic '{TOPIC}'")

    sample = [
        make_payload(101, "OK", "2025.1.0"),
        make_payload(102, "UPDATING", "2025.2.0"),
        make_payload(103, "ERROR", "2025.2.0"),
        make_payload(101, "OK", "2025.2.0"),
    ]

    for record in sample:
        p.poll(0)  # serve delivery callbacks
        p.produce(
            topic=TOPIC,
            key=str(record["vehicle_id"]),
            value=json.dumps(record).encode("utf-8"),
            callback=delivery_report
        )
        time.sleep(0.3)

    p.flush()
    print("Done.")

if __name__ == "__main__":
    main()
