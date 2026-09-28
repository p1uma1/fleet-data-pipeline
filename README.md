# Fleet Data Pipeline

Ride-hailing fleet operations. **Lambda architecture:** Kafka + Spark streaming (live utilization), Spark batch (daily profitability), PostgreSQL + API (serving).

Simulated clock: 1 day = 5 minutes.

| Person | Work | Folder |
|---|---|---|
| 1 | Simulator + Kafka producer + daily expense CSV | [`ingestion/`](ingestion/README.md) |
| 2 | Spark streaming + profitability job | [`streaming/`](streaming/README.md) |
| 3 | Airflow + PostgreSQL + API | [`orchenstrater/`](orchenstrater/README.md) |

**Kafka:** `localhost:9092` · topic `fleet.telemetry` (key = `vehicle_id`) · alerts `fleet.alerts`  
**Postgres:** `fleet` / `fleet` / `fleet` · tables in [`streaming/sql/analytics_tables.sql`](streaming/sql/analytics_tables.sql)

Telemetry:

```json
{"trip_id":"T001","driver_id":"D001","vehicle_id":"V001","lat":6.9271,"lon":79.8612,"speed":42.5,"status":"on_trip","fare":820.0,"timestamp":"2026-09-27T20:30:00"}
```

```powershell
copy .env.example .env
docker compose up -d --build
```
