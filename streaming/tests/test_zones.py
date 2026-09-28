from src.zones import (
    MAX_LAT,
    MAX_LON,
    MIN_LAT,
    MIN_LON,
    is_inside_city_bounds,
    time_of_day,
    zone_id,
)


def test_south_west_corner_is_z00():
    assert zone_id(MIN_LAT, MIN_LON) == "Z00"


def test_north_east_corner_is_z22():
    assert zone_id(MAX_LAT, MAX_LON) == "Z22"


def test_colombo_sample_event_maps_to_centre_cell():
    # Person 1 sample: 6.9271, 79.8612
    assert zone_id(6.9271, 79.8612) == "Z11"


def test_out_of_bounds_coordinates_clamp_into_grid():
    assert zone_id(6.0, 79.0) == "Z00"
    assert zone_id(8.0, 81.0) == "Z22"


def test_city_bounds_helper():
    assert is_inside_city_bounds(6.9271, 79.8612)
    assert not is_inside_city_bounds(None, 79.86)
    assert not is_inside_city_bounds(1.0, 1.0)


def test_time_of_day_buckets():
    assert time_of_day(4) == "night"
    assert time_of_day(8) == "morning"
    assert time_of_day(13) == "afternoon"
    assert time_of_day(19) == "evening"
    assert time_of_day(23) == "night"
