from datetime import datetime, timedelta, timezone

from src.alerts import (
    idle_minutes,
    is_no_data_alert,
    next_idle_since,
    should_emit_idle_alert,
    should_reset_idle_alert,
)


def test_consecutive_idle_keeps_original_idle_since():
    start = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
    later = start + timedelta(minutes=3)
    kept = next_idle_since("idle", start, "idle", later)
    assert kept == start


def test_idle_since_resets_when_vehicle_moves():
    start = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
    later = start + timedelta(minutes=3)
    assert next_idle_since("idle", start, "on_trip", later) is None


def test_idle_alert_fires_once_after_threshold():
    idle_since = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
    now = idle_since + timedelta(minutes=5)
    assert should_emit_idle_alert(idle_since, now, already_sent=False, threshold_minutes=5)
    assert not should_emit_idle_alert(idle_since, now, already_sent=True, threshold_minutes=5)
    assert idle_minutes(idle_since, now) == 5.0


def test_idle_alert_does_not_fire_before_threshold():
    idle_since = datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc)
    now = idle_since + timedelta(minutes=4, seconds=59)
    assert not should_emit_idle_alert(idle_since, now, already_sent=False, threshold_minutes=5)


def test_leaving_idle_resets_alert_flag():
    assert should_reset_idle_alert("on_trip")
    assert not should_reset_idle_alert("idle")


def test_no_data_alert_uses_batch_streak():
    assert not is_no_data_alert(11, 12)
    assert is_no_data_alert(12, 12)
