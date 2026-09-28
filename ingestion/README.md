# Person 1 — Ingestion

`simulator.py` is done (prints events). **Still needed:** Kafka producer and daily expense file.

Producer (`producer.py`):

- Broker: `localhost:9092`
- Topic: `fleet.telemetry` (3 partitions, key = `vehicle_id`)
- JSON: `trip_id, driver_id, vehicle_id, lat, lon, speed, status, fare, timestamp`
- `status` must be `idle | enroute | on_trip`

Daily file: `data/batch/expenses_YYYYMMDD.csv`

```text
vehicle_id,fuel_cost,maintenance_cost,distance_covered,service_flag
V001,4200.50,1500.00,180.4,false
```

Vehicle IDs must match the simulator (`V001`–`V010`).
