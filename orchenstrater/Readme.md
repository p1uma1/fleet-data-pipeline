# Person 3 — Airflow + PostgreSQL + API

Postgres is already in Docker (`localhost:5432`, db/user/pass `fleet`). Include [`../streaming/sql/analytics_tables.sql`](../streaming/sql/analytics_tables.sql) in your schema.

Airflow: when `data/batch/expenses_YYYYMMDD.csv` arrives, run:

```text
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.4
  /opt/project/streaming/jobs/batch_profitability.py
  --expenses-path /opt/project/data/batch/expenses_YYYYMMDD.csv
  --report-date YYYY-MM-DD
```

API should read these tables (do not recompute Spark logic):

| Endpoint | Table |
|---|---|
| `GET /metrics/fleet` | `fleet_metrics_realtime` |
| `GET /metrics/zones` | `zone_earnings_realtime` |
| `GET /alerts` | `idle_alerts` |
| `GET /health` | `pipeline_health` |
| `GET /reports/profitability?date=` | `vehicle_profitability` |
