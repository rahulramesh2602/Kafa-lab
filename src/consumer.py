import json
from confluent_kafka import Consumer, KafkaException, KafkaError

BROKER = "localhost:9092"
GROUP_ID = "vehicle-status-readers"
TOPIC = "vehicle-status"

def main():
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"  # read from beginning if no committed offset
    })
    print(f"Consumer connected to {BROKER}, subscribing to '{TOPIC}'")
    c.subscribe([TOPIC])

    try:
        while True:
            msg = c.poll(1.0)  # seconds
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            key = msg.key().decode("utf-8") if msg.key() else None
            data = json.loads(msg.value().decode("utf-8"))
            print(f"⬇️  key={key} data={data}")
    except KeyboardInterrupt:
        print("Stopping consumer…")
    finally:
        c.close()

if __name__ == "__main__":
    main()
