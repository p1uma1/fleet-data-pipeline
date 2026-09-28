# Person 2 — Spark streaming

Reads `fleet.telemetry`, writes live metrics and alerts to Postgres, then a daily profitability join.

```powershell
copy .env.example .env
docker compose up -d --build
powershell -File streaming/scripts/submit-stream.ps1
python streaming/dev/publish_sample.py
```

Until Person 1 finishes `producer.py`, `publish_sample.py` is only for testing.

Batch job (Person 3 schedules this in Airflow):

```powershell
powershell -File streaming/scripts/submit-batch.ps1 /opt/project/data/batch/expenses_20260927.csv.example 2026-09-27
```

Tables: `telemetry_silver`, `fleet_metrics_realtime`, `zone_earnings_realtime`, `idle_alerts`, `pipeline_health`, `vehicle_profitability`.
