"""Dev-only Kafka publisher so Person 2 can test Spark before Person 1 finishes producer.py.

This is not a replacement for ingestion/producer.py.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

STREAMING_ROOT = Path(__file__).resolve().parents[1]
if str(STREAMING_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMING_ROOT))

from src.config import settings  # noqa: E402
from src.logging_utils import get_logger, log_event  # noqa: E402

logger = get_logger("publish_sample", stage="ingestion")

SAMPLE_VEHICLES = [
    {
        "trip_id": "T00001",
        "driver_id": "D001",
        "vehicle_id": "V001",
        "lat": 6.9271,
        "lon": 79.8612,
        "speed": 32.5,
        "status": "on_trip",
        "fare": 420.0,
    },
    {
        "trip_id": None,
        "driver_id": "D002",
        "vehicle_id": "V002",
        "lat": 6.88,
        "lon": 79.83,
        "speed": 0.0,
        "status": "idle",
        "fare": 0.0,
    },
    {
        "trip_id": "T00002",
        "driver_id": "D003",
        "vehicle_id": "V003",
        "lat": 6.97,
        "lon": 79.93,
        "speed": 18.0,
        "status": "enroute",
        "fare": 0.0,
    },
]


def build_event(template: dict) -> dict:
    event = dict(template)
    event["timestamp"] = datetime.now(timezone.utc).isoformat()
    return event


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish sample fleet telemetry to Kafka")
    parser.add_argument("--once", action="store_true", help="Send one burst and exit")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between bursts")
    parser.add_argument("--count", type=int, default=0, help="Stop after N bursts (0 = forever)")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    log_event(
        logger,
        "publishing_sample_events",
        bootstrap=settings.kafka_bootstrap,
        topic=settings.telemetry_topic,
    )

    bursts = 0
    try:
        while True:
            for template in SAMPLE_VEHICLES:
                event = build_event(template)
                producer.send(
                    settings.telemetry_topic,
                    key=event["vehicle_id"],
                    value=event,
                )
            producer.flush()
            bursts += 1
            log_event(logger, "sample_burst_sent", burst=bursts, events=len(SAMPLE_VEHICLES))
            if args.once or (args.count and bursts >= args.count):
                break
            time.sleep(args.interval)
    except KafkaError:
        logger.exception("kafka publish failed")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    main()
