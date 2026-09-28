# Person 3 — Airflow + PostgreSQL + API

Owner: Person 3. Person 2 already writes the analytics tables. This folder is for orchestration and serving.

The directory name `orchenstrater` is a typo of “orchestrator”. Leave it unless the whole group agrees to rename (it would break existing clones).

## What you own

1. PostgreSQL as the serving store (Compose already runs Postgres `fleet/fleet/fleet` on `localhost:5432`).
2. Schema: include Person 2’s file [`../streaming/sql/analytics_tables.sql`](../streaming/sql/analytics_tables.sql) in [`../data-structure/database-schema.sql`](../data-structure/database-schema.sql). That file is still empty.
3. Apache Airflow DAGs:
   - Detect Person 1’s daily file `data/batch/expenses_YYYYMMDD.csv` (simulated day = 5 minutes is acceptable).
   - `spark-submit` Person 2’s batch job:

     ```text
     /opt/spark/bin/spark-submit
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.4
       /opt/project/streaming/jobs/batch_profitability.py
       --expenses-path /opt/project/data/batch/expenses_YYYYMMDD.csv
       --report-date YYYY-MM-DD
     ```

   - Emit or archive the daily per-vehicle profitability report from `vehicle_profitability`.
4. REST API (suggested by the brief):

   | Endpoint | Source table |
   |---|---|
   | `GET /metrics/fleet` | `fleet_metrics_realtime` — active vehicles, idle ratio, trips/hour |
   | `GET /metrics/zones` | `zone_earnings_realtime` — earnings by area / time-of-day |
   | `GET /alerts` | `idle_alerts` — idle vehicles and `NO_DATA` |
   | `GET /health` | `pipeline_health` |
   | `GET /reports/profitability?date=` | `vehicle_profitability` |

5. Observability on the serving layer: log API errors, and treat `pipeline_health.status = 'NO_DATA'` as a health-check failure.

## Do not reimplement

- Spark windowing and idle detection — already in [`../streaming/`](../streaming/README.md)
- Kafka producers — Person 1

## Local dependency

```powershell
docker compose up -d postgres spark kafka
```

Person 2’s streaming job must be running for `/metrics/fleet` to show live numbers.
