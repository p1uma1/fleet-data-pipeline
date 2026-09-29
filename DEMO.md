# Live Demo Script — Fleet Data Pipeline (A to Z)

**Duration:** 5–10 minutes  
**Use case:** Ride-Hailing Fleet Operations (EC8203)  
**Architecture:** Lambda (Kafka + Spark streaming + Spark batch + PostgreSQL + API + Airflow)

Simulated clock: **1 day = 5 minutes**.

---

## What you will show

| Step | Layer | What the audience sees |
|---|---|---|
| 1 | Infra | Docker stack healthy |
| 2 | Ingestion | Telemetry events entering Kafka |
| 3 | Speed | Spark Structured Streaming updating live metrics |
| 4 | Observability | Pipeline health + idle / no-data alerts |
| 5 | Serving | FastAPI returning real-time KPIs |
| 6 | Batch | Airflow runs daily profitability join |
| 7 | Report | Unprofitable vehicles via API |

---

## Before the demo (do once, at home)

### 0. Prerequisites

- Docker Desktop installed and **running**
- PowerShell
- Python 3.10+ with: `pip install kafka-python python-dotenv`
- Browser
- Repo at `E:\BigDataProject` on branch `main`

### 1. Open 4 terminals

Keep this layout during the demo:

| Terminal | Role |
|---|---|
| T1 | Docker / status |
| T2 | Spark streaming job (long-running) |
| T3 | Kafka event publisher |
| T4 | API / SQL / curl checks |

### 2. Start the stack (cold start can take 3–8 minutes)

```powershell
cd E:\BigDataProject
copy .env.example .env
docker compose up -d --build
```

Wait until services are up:

```powershell
docker compose ps
```

Expected: `kafka`, `postgres`, `spark`, `api`, `airflow-webserver`, `airflow-scheduler` are up/healthy.  
`kafka-init` and `airflow-init` may show `Exited (0)` — that is normal.

### 3. Confirm ports

| Service | URL / port |
|---|---|
| FastAPI docs | http://localhost:8000/docs |
| Airflow UI | http://localhost:8080 (`admin` / `admin`) |
| Kafka | `localhost:9092` |
| Postgres (host) | `localhost:5434` (user/pass/db = `fleet`) |

### 4. Pre-warm check (optional)

```powershell
curl http://localhost:8000/health
curl http://localhost:8000/db-health
```

If both return `"ok"` / `"connected"`, you are demo-ready.

---

## Live demo script (say + do)

### Minute 0 — Intro (30 sec)

**Say:**

> We built a Lambda pipeline for a ride-hailing fleet.  
> Streaming GPS events give live utilization by zone.  
> A daily expense file is joined for per-vehicle profitability.  
> We will show ingestion → Spark → Postgres → API → Airflow → alerts.

**Do:** show root `README.md` architecture table (optional, 5 sec).

---

### Minute 1 — Start Spark speed layer (T2)

**Say:**

> Person 2’s Spark Structured Streaming job reads Kafka topic `fleet.telemetry`, cleans events, windows them, writes live metrics, and emits alerts.

**Do (T2):**

```powershell
cd E:\BigDataProject
powershell -File streaming\scripts\submit-stream.ps1
```

Leave this running. First package download may take 1–2 minutes. Wait until you see JSON logs like `starting_speed_layer` / `query_started`.

---

### Minute 2 — Ingest live telemetry (T3)

**Say:**

> Person 1’s producer path publishes GPS events every few seconds. For this demo we use the sample publisher that sends the same JSON contract to Kafka.

**Do (T3):**

```powershell
cd E:\BigDataProject
python streaming\dev\publish_sample.py
```

Leave it running. You should see `sample_burst_sent` every ~2 seconds.

**Event contract (show once):**

```json
{
  "trip_id": "T00001",
  "driver_id": "D001",
  "vehicle_id": "V001",
  "lat": 6.9271,
  "lon": 79.8612,
  "speed": 32.5,
  "status": "on_trip",
  "fare": 420.0,
  "timestamp": "2026-09-27T..."
}
```

Optional Kafka proof (T1, if needed):

```powershell
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fleet.telemetry --from-beginning --max-messages 3
```

---

### Minute 3 — Show Spark processing + observability (T2 + T4)

**Say:**

> Spark is cleaning, zoning Colombo into a 3×3 grid, computing active vehicles / idle ratio / trips per hour, and writing to PostgreSQL. Pipeline health is updated every micro-batch.

**Do (T4) — Postgres checks:**

```powershell
docker compose exec postgres psql -U fleet -d fleet -c "SELECT status, last_input_rows, empty_batch_streak, message, updated_at FROM pipeline_health;"
```

```powershell
docker compose exec postgres psql -U fleet -d fleet -c "SELECT active_vehicles, idle_vehicles, idle_ratio, trips_per_hour, live_fare_exposure, updated_at FROM fleet_metrics_realtime;"
```

```powershell
docker compose exec postgres psql -U fleet -d fleet -c "SELECT zone_id, time_of_day, active_vehicles, live_fare_exposure FROM zone_earnings_realtime ORDER BY zone_id;"
```

**Point at T2 logs:** show `batch_processed` with `input_rows` and `idle_alerts`.

---

### Minute 4 — Serving layer API (browser + T4)

**Say:**

> Person 3’s FastAPI serves the same tables. This is the live dashboard/API surface for operations.

**Do:** open http://localhost:8000/docs and hit:

1. `GET /fleet/realtime`
2. `GET /zones/earnings`
3. `GET /pipeline/health`
4. `GET /alerts`

Or from T4:

```powershell
curl http://localhost:8000/fleet/realtime
curl http://localhost:8000/zones/earnings
curl http://localhost:8000/pipeline/health
curl "http://localhost:8000/alerts?limit=10"
```

**Say what the numbers mean:**

- `active_vehicles` — not idle right now  
- `idle_ratio` — idle / total  
- `trips_per_hour` — extrapolated from the current window  
- `live_fare_exposure` — current trip fare exposure by zone  

---

### Minute 5 — Observability: idle + no-data alerts

#### A) Idle vehicle alert (optional, ~1 min if preconfigured)

If you want a visible idle alert during the demo, restart Spark with a short threshold **before** the demo:

In `docker-compose.yml` under `spark.environment`, temporarily add:

```yaml
IDLE_THRESHOLD_MINUTES: "0.2"
```

Then:

```powershell
docker compose up -d spark
```

Re-run `submit-stream.ps1`. Sample publisher keeps `V002` idle, so an `IDLE_VEHICLE` alert should appear.

Check:

```powershell
curl "http://localhost:8000/alerts?limit=10"
docker compose exec postgres psql -U fleet -d fleet -c "SELECT alert_type, vehicle_id, idle_minutes, message, created_at FROM idle_alerts ORDER BY created_at DESC LIMIT 5;"
```

#### B) No-data health alert (strong observability demo)

**Say:**

> Assignment requires a health rule when no data arrives for N minutes. Our Spark job fires `NO_DATA` after ~2 minutes of empty batches.

**Do:**

1. Stop the publisher in T3: `Ctrl + C`
2. Wait ~2 minutes (or narrate while you show the streak climbing)
3. Check:

```powershell
curl http://localhost:8000/pipeline/health
curl "http://localhost:8000/alerts?limit=5"
```

Expect `status: NO_DATA` and an alert message about empty batches.

4. Restart publisher to recover:

```powershell
python streaming\dev\publish_sample.py
```

Health should return to `HEALTHY`.

---

### Minute 6–7 — Batch layer via Airflow

**Say:**

> Speed layer answered “right now”. Batch layer answers “which vehicles are unprofitable after fuel/maintenance”. Airflow detects the daily expense CSV and spark-submits Person 2’s profitability job.

**Do:**

1. Confirm expense file exists:

```powershell
dir data\batch\expenses_20260927.csv.example
```

2. Open Airflow: http://localhost:8080  
   Login: `admin` / `admin`

3. Open DAG `fleet_profitability_pipeline` → **Trigger DAG w/ config**

Use this JSON (also in `run_conf.json`):

```json
{
  "report_date": "2026-09-27"
}
```

4. Watch tasks go green:

`check_expense_file` → `validate_expense_csv` → `run_spark_profitability` → `verify_profitability_rows`

5. Show results:

```powershell
curl "http://localhost:8000/profitability?report_date=2026-09-27"
```

```powershell
docker compose exec postgres psql -U fleet -d fleet -c "SELECT vehicle_id, earnings, fuel_cost, maintenance_cost, profit, unprofitable FROM vehicle_profitability WHERE report_date = '2026-09-27' ORDER BY profit;"
```

**Say:**

> Negative profit → `unprofitable = true`. That is the daily reconciliation report the brief asks for.

---

### Minute 8 — Closing (30–45 sec)

**Say:**

> End-to-end:  
> Kafka ingest → Spark streaming metrics/alerts → PostgreSQL → FastAPI for live ops → Airflow + Spark batch for daily profitability.  
> Observability covers structured Spark logs, `pipeline_health`, and threshold alerts for idle vehicles and no data.

Mention one trade-off if asked:

> Lambda fits because expenses arrive as a daily file, not a continuous stream. Kappa would force that batch feed into Kafka without helping the business question.

---

## Quick command cheat sheet (copy/paste)

```powershell
# Start
cd E:\BigDataProject
docker compose up -d --build
docker compose ps

# Spark streaming (T2)
powershell -File streaming\scripts\submit-stream.ps1

# Telemetry (T3)
python streaming\dev\publish_sample.py

# API checks (T4)
curl http://localhost:8000/health
curl http://localhost:8000/db-health
curl http://localhost:8000/fleet/realtime
curl http://localhost:8000/zones/earnings
curl http://localhost:8000/pipeline/health
curl "http://localhost:8000/alerts?limit=10"
curl "http://localhost:8000/profitability?report_date=2026-09-27"

# SQL checks
docker compose exec postgres psql -U fleet -d fleet -c "SELECT * FROM pipeline_health;"
docker compose exec postgres psql -U fleet -d fleet -c "SELECT * FROM fleet_metrics_realtime;"
docker compose exec postgres psql -U fleet -d fleet -c "SELECT alert_type, vehicle_id, message FROM idle_alerts ORDER BY created_at DESC LIMIT 5;"
docker compose exec postgres psql -U fleet -d fleet -c "SELECT vehicle_id, profit, unprofitable FROM vehicle_profitability WHERE report_date='2026-09-27' ORDER BY profit;"

# Stop publisher: Ctrl+C in T3
# Stop spark job: Ctrl+C in T2
# Stop stack:
docker compose down
```

---

## URLs to keep open during demo

1. http://localhost:8000/docs — API  
2. http://localhost:8080 — Airflow (`admin` / `admin`)  
3. Terminal T2 — Spark logs  
4. Terminal T3 — Kafka publisher logs  

---

## If something breaks (fast fixes)

| Problem | Fix |
|---|---|
| `docker` not found | Start Docker Desktop; reopen PowerShell |
| Ports busy (`8000`, `8080`, `9092`, `5434`) | Stop old containers: `docker compose down` |
| Spark stuck downloading packages | Wait; first run caches jars |
| API empty metrics | Confirm T2 Spark + T3 publisher are running |
| Airflow DAG missing | Wait ~30s after compose up; refresh UI |
| Airflow Spark task fails | Ensure container name `fleet-spark` is up: `docker ps` |
| Stale Spark checkpoint error | Delete `checkpoints\` then resubmit stream |
| Postgres connection from host fails | Host port is **5434**, not 5432 |

Reset soft state (keeps images):

```powershell
docker compose down
Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue
docker compose up -d --build
```

Hard reset (wipes DB volumes — only if needed):

```powershell
docker compose down -v
docker compose up -d --build
```

---

## Demo roles (who clicks what)

| Person | Live responsibility |
|---|---|
| Person 1 | Explain event schema; run / narrate Kafka ingest |
| Person 2 | Run Spark job; explain windows, zones, alerts, logs |
| Person 3 | Show API + Airflow DAG + profitability report |

---

## After the demo

```powershell
# Stop publisher and Spark with Ctrl+C, then:
docker compose down
```

Keep containers up if you still need screenshots for the report.
