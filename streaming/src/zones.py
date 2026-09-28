"""Colombo grid zones and time-of-day buckets.

Person 1's simulator uses these GPS bounds:
  lat 6.85-7.00, lon 79.80-79.95
"""

from __future__ import annotations

from datetime import datetime

MIN_LAT = 6.85
MAX_LAT = 7.00
MIN_LON = 79.80
MAX_LON = 79.95
GRID_SIZE = 3

TIME_OF_DAY_BUCKETS = ("night", "morning", "afternoon", "evening")


def _clamp(value: float, low: float, high: float) -> float:
    if value < low:
        return low
    # Keep the max edge inside the last cell.
    if value >= high:
        return high - 1e-12
    return value


def zone_row(lat: float) -> int:
    span = (MAX_LAT - MIN_LAT) / GRID_SIZE
    row = int((_clamp(lat, MIN_LAT, MAX_LAT) - MIN_LAT) / span)
    return min(max(row, 0), GRID_SIZE - 1)


def zone_col(lon: float) -> int:
    span = (MAX_LON - MIN_LON) / GRID_SIZE
    col = int((_clamp(lon, MIN_LON, MAX_LON) - MIN_LON) / span)
    return min(max(col, 0), GRID_SIZE - 1)


def zone_id(lat: float | None, lon: float | None) -> str | None:
    if lat is None or lon is None:
        return None
    return f"Z{zone_row(float(lat))}{zone_col(float(lon))}"


def is_inside_city_bounds(lat: float | None, lon: float | None) -> bool:
    if lat is None or lon is None:
        return False
    return MIN_LAT <= lat <= MAX_LAT and MIN_LON <= lon <= MAX_LON


def time_of_day(hour: int) -> str:
    if 5 <= hour < 12:
        return "morning"
    if 12 <= hour < 17:
        return "afternoon"
    if 17 <= hour < 21:
        return "evening"
    return "night"


def time_of_day_from_ts(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    return time_of_day(ts.hour)
