# Person 1 — Data generation + Kafka ingestion

Owner: Person 1. Person 2 consumes this data; do not skip the contracts below.

## Already done

[`simulator.py`](simulator.py) emits Colombo GPS telemetry every 2 seconds for 10 drivers:

```json
{
  "trip_id": "T001",
  "driver_id": "D001",
  "vehicle_id": "V001",
  "lat": 6.9271,
  "lon": 79.8612,
  "speed": 42.5,
  "status": "on_trip",
  "fare": 820.0,
  "timestamp": "2026-09-27T20:30:00"
}
```

Statuses must stay `idle | enroute | on_trip`. Fare is treated as **cumulative per trip** (Spark takes `max(fare)` per `trip_id`).

The simulator currently **prints to stdout**. It is not yet on Kafka. [`producer.py`](producer.py) is empty.

## Remaining work

### 1. Kafka producer (`producer.py`)

Wire the simulator into Kafka. Docker Compose already creates the topics.

| Setting | Value |
|---|---|
| Bootstrap (from host) | `localhost:9092` |
| Topic | `fleet.telemetry` |
| Partitions | 3 |
| Record key | `vehicle_id` |
| Value | JSON object with the fields above |
| Timestamp | UTC ISO-8601, generated at emit time |

Suggested shape: `simulator.generate_data()` → `producer.send(topic, key=vehicle_id, value=json)`. Keep producing until stopped.

### 2. Daily batch source (required by the brief)

Once per **simulated day** (group assumption: 1 day = 5 minutes), drop a CSV:

`data/batch/expenses_YYYYMMDD.csv`

```text
vehicle_id,fuel_cost,maintenance_cost,distance_covered,service_flag
V001,4200.50,1500.00,180.4,false
```

Fill [`../data-structure/expense-schema.csv`](../data-structure/expense-schema.csv) with that header plus one example row.

Vehicle IDs must match the simulator (`V001`–`V010`) so Person 2’s profitability join works.

A checked-in example Person 2 uses for local tests:

[`../data/batch/expenses_20260927.csv.example`](../data/batch/expenses_20260927.csv.example)

### 3. Observability on the producer

Structured logs: records sent, send errors, last event timestamp. Person 2’s Spark job alerts if **no Kafka data arrives for ~2 minutes** (`NO_DATA`), so a silent producer looks like a pipeline failure.

## How Person 2 tests without this producer

`python streaming/dev/publish_sample.py` publishes a few static events. That is a Spark test fixture, not your ingestion path. Replace it with `producer.py` before the demo.

## Local Kafka

```powershell
docker compose up -d kafka kafka-init
```
