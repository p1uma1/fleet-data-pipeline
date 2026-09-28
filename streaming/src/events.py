"""Spark-free event contract and field rules shared by tests and Spark jobs."""

from __future__ import annotations

TELEMETRY_FIELDS = (
    "trip_id",
    "driver_id",
    "vehicle_id",
    "lat",
    "lon",
    "speed",
    "status",
    "fare",
    "timestamp",
)

VALID_STATUSES = ("idle", "enroute", "on_trip")


def normalize_status(status: str | None) -> str | None:
    if status is None:
        return None
    value = str(status).strip().lower()
    return value if value in VALID_STATUSES else None
