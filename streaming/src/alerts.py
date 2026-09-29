"""Pure alert rules so they can be unit-tested without Spark."""

from __future__ import annotations

from datetime import datetime, timezone


def _as_utc(value: datetime) -> datetime:
    """Normalize naive or timezone-aware datetimes to UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def next_idle_since(
    previous_status: str | None,
    previous_idle_since: datetime | None,
    new_status: str,
    new_ts: datetime,
) -> datetime | None:
    """Keep idle_since on consecutive idle events; reset when the vehicle moves."""
    if new_status == "idle":
        if previous_status == "idle" and previous_idle_since is not None:
            return _as_utc(previous_idle_since)

        return _as_utc(new_ts)

    return None


def idle_minutes(idle_since: datetime | None, event_ts: datetime) -> float:
    if idle_since is None:
        return 0.0

    idle_since_utc = _as_utc(idle_since)
    event_ts_utc = _as_utc(event_ts)

    return max(
        (event_ts_utc - idle_since_utc).total_seconds() / 60.0,
        0.0,
    )


def should_emit_idle_alert(
    idle_since: datetime | None,
    event_ts: datetime,
    already_sent: bool,
    threshold_minutes: float = 5.0,
) -> bool:
    """Rising-edge alert: fire once when idle time first crosses the threshold."""
    if already_sent or idle_since is None:
        return False

    return idle_minutes(idle_since, event_ts) >= threshold_minutes


def should_reset_idle_alert(new_status: str) -> bool:
    return new_status != "idle"


def is_no_data_alert(empty_batch_streak: int, threshold_batches: int) -> bool:
    return empty_batch_streak >= threshold_batches
