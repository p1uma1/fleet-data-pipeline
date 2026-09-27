import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone


# ----------------------------
# Configuration
# ----------------------------

NUM_DRIVERS = 10
EVENT_INTERVAL_SECONDS = 2

MIN_LAT = 6.85
MAX_LAT = 7.00

MIN_LON = 79.80
MAX_LON = 79.95

TRIP_START_PROBABILITY = 0.15

MIN_SPEED_KMH = 15
MAX_SPEED_KMH = 45

ARRIVAL_THRESHOLD = 0.0003


# ----------------------------
# Models
# ----------------------------

@dataclass
class Location:
    lat: float
    lon: float


@dataclass
class Trip:
    trip_id: str
    pickup: Location
    destination: Location
    fare: float = 0.0
    distance_travelled: float = 0.0


@dataclass
class Driver:
    driver_id: str
    vehicle_id: str
    location: Location
    speed: float = 0.0
    status: str = "idle"
    current_trip: Trip | None = None


# ----------------------------
# Global trip counter
# ----------------------------

trip_counter = 1


# ----------------------------
# Initialization
# ----------------------------

def random_location():
    return Location(
        lat=random.uniform(MIN_LAT, MAX_LAT),
        lon=random.uniform(MIN_LON, MAX_LON)
    )


def initialize_drivers(number):
    drivers = []

    for i in range(1, number + 1):

        driver = Driver(
            driver_id=f"D{i:03}",
            vehicle_id=f"V{i:03}",
            location=random_location()
        )

        drivers.append(driver)

    return drivers


# ----------------------------
# Trip creation
# ----------------------------

def create_trip():
    global trip_counter

    trip_id = f"T{trip_counter:05}"
    trip_counter += 1

    return Trip(
        trip_id=trip_id,
        pickup=random_location(),
        destination=random_location()
    )


def assign_trip(driver):
    if driver.status != "idle":
        return

    driver.current_trip = create_trip()
    driver.status = "enroute"
    driver.speed = random.uniform(MIN_SPEED_KMH, MAX_SPEED_KMH)


# ----------------------------
# Distance / movement
# ----------------------------

def distance_between(a, b):
    return math.sqrt(
        (b.lat - a.lat) ** 2 +
        (b.lon - a.lon) ** 2
    )


def move_towards(driver, target):
    delta_lat = target.lat - driver.location.lat
    delta_lon = target.lon - driver.location.lon

    distance = math.sqrt(
        delta_lat ** 2 +
        delta_lon ** 2
    )

    if distance == 0:
        return

    direction_lat = delta_lat / distance
    direction_lon = delta_lon / distance

    # Small movement step based on speed.
    # This is simulation-scale movement, not exact geography.
    movement_step = driver.speed * 0.000005

    driver.location.lat += direction_lat * movement_step
    driver.location.lon += direction_lon * movement_step

    if driver.current_trip:
        driver.current_trip.distance_travelled += movement_step


# ----------------------------
# Driver state update
# ----------------------------

def update_driver(driver):

    # ----------------
    # IDLE
    # ----------------

    if driver.status == "idle":

        driver.speed = 0

        if random.random() < TRIP_START_PROBABILITY:
            assign_trip(driver)

        return


    # ----------------
    # ENROUTE
    # ----------------

    if driver.status == "enroute":

        trip = driver.current_trip

        if trip is None:
            driver.status = "idle"
            return

        driver.speed += random.uniform(-2, 2)

        driver.speed = max(
            MIN_SPEED_KMH,
            min(driver.speed, MAX_SPEED_KMH)
        )

        move_towards(
            driver,
            trip.pickup
        )

        distance = distance_between(
            driver.location,
            trip.pickup
        )

        if distance < ARRIVAL_THRESHOLD:

            driver.location = trip.pickup

            driver.status = "on_trip"

            driver.speed = random.uniform(
                MIN_SPEED_KMH,
                MAX_SPEED_KMH
            )

        return


    # ----------------
    # ON TRIP
    # ----------------

    if driver.status == "on_trip":

        trip = driver.current_trip

        if trip is None:
            driver.status = "idle"
            return

        driver.speed += random.uniform(-2, 2)

        driver.speed = max(
            MIN_SPEED_KMH,
            min(driver.speed, MAX_SPEED_KMH)
        )

        move_towards(
            driver,
            trip.destination
        )

        # Simple simulated fare growth
        trip.fare += driver.speed * 0.8

        distance = distance_between(
            driver.location,
            trip.destination
        )

        if distance < ARRIVAL_THRESHOLD:

            driver.location = trip.destination

            trip.fare += 200

            driver.status = "idle"

            driver.speed = 0

            # We'll keep the trip for one final event
            # and remove it after event creation.

        return


# ----------------------------
# Event generation
# ----------------------------

def create_event(driver):

    trip_id = None
    fare = 0.0

    if driver.current_trip:

        trip_id = driver.current_trip.trip_id
        fare = round(driver.current_trip.fare, 2)

    event = {
        "trip_id": trip_id,
        "driver_id": driver.driver_id,
        "vehicle_id": driver.vehicle_id,
        "lat": round(driver.location.lat, 6),
        "lon": round(driver.location.lon, 6),
        "speed": round(driver.speed, 2),
        "status": driver.status,
        "fare": fare,
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }

    return event


# ----------------------------
# Data generation loop
# ----------------------------

def generate_data(drivers):

    events = []

    for driver in drivers:

        previous_status = driver.status

        update_driver(driver)

        event = create_event(driver)

        events.append(event)

        # If trip just finished,
        # clear it after emitting final state
        if (
            previous_status == "on_trip"
            and driver.status == "idle"
        ):
            driver.current_trip = None

    return events


# ----------------------------
# Main
# ----------------------------

def main():

    drivers = initialize_drivers(
        NUM_DRIVERS
    )

    while True:

        events = generate_data(
            drivers
        )

        for event in events:
            print(event)

        print("-" * 80)

        time.sleep(
            EVENT_INTERVAL_SECONDS
        )


if __name__ == "__main__":
    main()