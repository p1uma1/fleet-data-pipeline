import os
import json
import time
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

from simulator import (
    initialize_drivers,
    generate_data,
    NUM_DRIVERS,
    EVENT_INTERVAL_SECONDS
)

# Load .env from repo root for local development (no-op inside Docker where
# environment variables are injected by docker-compose).
load_dotenv(Path(__file__).resolve().parents[1] / ".env")


KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "localhost:9092"
)

TOPIC_NAME = os.getenv(
    "TELEMETRY_TOPIC",
    "fleet.telemetry"
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,

        key_serializer=lambda key: key.encode("utf-8"),

        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),

        acks="all",
        retries=5
    )


def main():
    producer = create_producer()

    drivers = initialize_drivers(
        NUM_DRIVERS
    )

    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC_NAME}")

    try:
        while True:

            events = generate_data(
                drivers
            )

            for event in events:

                future = producer.send(
                    TOPIC_NAME,
                    key=event["vehicle_id"],
                    value=event
                )

                metadata = future.get(
                    timeout=10
                )

                print(
                    f"{event['vehicle_id']} ({event['driver_id']}) -> "
                    f"partition={metadata.partition}, "
                    f"offset={metadata.offset}"
                )

            producer.flush()

            time.sleep(
                EVENT_INTERVAL_SECONDS
            )

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()