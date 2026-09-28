from src.events import VALID_STATUSES, normalize_status


def test_normalize_status_accepts_known_values():
    assert normalize_status("IDLE") == "idle"
    assert normalize_status(" Enroute ") == "enroute"
    assert normalize_status("on_trip") == "on_trip"
    assert set(VALID_STATUSES) == {"idle", "enroute", "on_trip"}


def test_normalize_status_rejects_unknown_values():
    assert normalize_status("offline") is None
    assert normalize_status(None) is None
    assert normalize_status("") is None
