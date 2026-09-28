"""Spark schemas and Kafka JSON contract for fleet telemetry."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.events import TELEMETRY_FIELDS, VALID_STATUSES  # noqa: F401

# Matches Person 1's Kafka JSON event.
telemetry_schema = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("fare", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

alert_schema = StructType(
    [
        StructField("alert_type", StringType(), False),
        StructField("vehicle_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("zone_id", StringType(), True),
        StructField("idle_minutes", DoubleType(), True),
        StructField("severity", StringType(), False),
        StructField("message", StringType(), False),
        StructField("created_at", StringType(), False),
    ]
)
