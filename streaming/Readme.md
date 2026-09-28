# Person 2 — Spark Structured Streaming + real-time analytics

This folder is the Lambda **speed layer** plus the Spark **batch profitability** job. It does not include Kafka producers or the REST API.

## What it does

1. Reads `fleet.telemetry` from Kafka (Person 1 JSON contract).
2. Cleans events, clamps GPS to Colombo, assigns a 3×3 `zone_id` (`Z00`–`Z22`) and a time-of-day bucket.
3. Writes cleaned rows to `telemetry_silver`.
4. Upserts **right now** KPIs:
   - `fleet_metrics_realtime` — active vehicles, idle ratio, trips/hour, live fare exposure
   - `zone_earnings_realtime` — the same metrics by area and time-of-day
5. Maintains 1-minute tumbling windows (`fleet_metrics_windows`) with a 2-minute watermark.
6. Tracks per-vehicle idle time. Fires `IDLE_VEHICLE` once a vehicle has been idle for 5 minutes (rising-edge, not a spam loop).
7. Fires `NO_DATA` if 12 consecutive 10-second batches are empty (~2 minutes).
8. Batch job joins daily expenses with `max(fare)` per trip and writes `vehicle_profitability`.

## Layout

```text
streaming/
  jobs/stream_fleet_analytics.py   # speed layer
  jobs/batch_profitability.py      # daily join
  src/                             # schema, zones, transforms, sinks, alerts
  sql/analytics_tables.sql         # tables this job writes
  dev/publish_sample.py            # test publisher only
  tests/                           # Spark-free unit tests
  scripts/                         # spark-submit helpers
```

## Run

From the repo root:

```powershell
copy .env.example .env
docker compose up -d --build
```

Unit tests:

```powershell
pip install -r streaming/requirements.txt
pytest streaming
```

Start the speed layer:

```powershell
powershell -File streaming/scripts/submit-stream.ps1
```

In a second terminal, publish sample events until Person 1’s producer is ready:

```powershell
pip install kafka-python python-dotenv
python streaming/dev/publish_sample.py
```

To demo idle alerts faster, set `IDLE_THRESHOLD_MINUTES=0.2` in the Spark service environment and recreate the container.

Batch profitability (Person 3 will schedule this from Airflow):

```powershell
powershell -File streaming/scripts/submit-batch.ps1 /opt/project/data/batch/expenses_20260927.csv.example 2026-09-27
```

Verify in Postgres (`psql` user/password/db = `fleet`):

```sql
SELECT * FROM fleet_metrics_realtime;
SELECT * FROM zone_earnings_realtime;
SELECT * FROM pipeline_health;
SELECT * FROM idle_alerts ORDER BY created_at DESC LIMIT 20;
SELECT * FROM vehicle_profitability;
```

## Metrics meaning

| Field | Meaning |
|---|---|
| `active_vehicles` | Distinct vehicles whose latest status is not `idle` |
| `idle_ratio` | idle vehicles / total vehicles in the snapshot |
| `trips_per_hour` | distinct `on_trip` trip IDs in the 10s batch, extrapolated to an hour |
| `live_fare_exposure` | sum of `max(fare)` per `trip_id` currently in view (fare is cumulative in the simulator) |
| `zone_id` | 3×3 grid over lat 6.85–7.00, lon 79.80–79.95 |

## Checkpoints

`checkpoints/stream_events` and `checkpoints/stream_windows` (gitignored). Delete these directories if you change the query graph and Spark refuses to start.

## Person 3 contract

Read from PostgreSQL; do not recompute Spark aggregations in the API.

Suggested endpoints:

- `GET /metrics/fleet` → `fleet_metrics_realtime`
- `GET /metrics/zones` → `zone_earnings_realtime`
- `GET /alerts` → `idle_alerts`
- `GET /reports/profitability?date=YYYY-MM-DD` → `vehicle_profitability`

Airflow should `spark-submit` `streaming/jobs/batch_profitability.py` after Person 1 drops the daily CSV.
