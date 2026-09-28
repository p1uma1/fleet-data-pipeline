-- Speed-layer and batch-layer tables written by Person 2 Spark jobs.
-- Person 3 should include this file in the serving-layer schema.

CREATE TABLE IF NOT EXISTS telemetry_silver (
    event_id            BIGSERIAL PRIMARY KEY,
    trip_id             TEXT,
    driver_id           TEXT NOT NULL,
    vehicle_id          TEXT NOT NULL,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    speed               DOUBLE PRECISION,
    status              TEXT NOT NULL,
    fare                NUMERIC(12, 2),
    event_ts            TIMESTAMPTZ NOT NULL,
    zone_id             TEXT,
    time_of_day         TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_silver_vehicle_ts
    ON telemetry_silver (vehicle_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_telemetry_silver_trip
    ON telemetry_silver (trip_id);

-- Latest 10-second snapshot answering "right now".
CREATE TABLE IF NOT EXISTS fleet_metrics_realtime (
    snapshot_id         INTEGER PRIMARY KEY DEFAULT 1 CHECK (snapshot_id = 1),
    window_start        TIMESTAMPTZ,
    window_end          TIMESTAMPTZ,
    active_vehicles     INTEGER NOT NULL DEFAULT 0,
    idle_vehicles       INTEGER NOT NULL DEFAULT 0,
    total_vehicles      INTEGER NOT NULL DEFAULT 0,
    idle_ratio          NUMERIC(8, 4) NOT NULL DEFAULT 0,
    trips_in_window     INTEGER NOT NULL DEFAULT 0,
    trips_per_hour      NUMERIC(12, 2) NOT NULL DEFAULT 0,
    avg_speed           NUMERIC(8, 2),
    live_fare_exposure  NUMERIC(12, 2) NOT NULL DEFAULT 0,
    event_count         BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO fleet_metrics_realtime (snapshot_id)
VALUES (1)
ON CONFLICT (snapshot_id) DO NOTHING;

-- Latest per-zone snapshot for earnings by area / time-of-day.
CREATE TABLE IF NOT EXISTS zone_earnings_realtime (
    zone_id             TEXT NOT NULL,
    time_of_day         TEXT NOT NULL,
    window_start        TIMESTAMPTZ,
    window_end          TIMESTAMPTZ,
    active_vehicles     INTEGER NOT NULL DEFAULT 0,
    trips_in_window     INTEGER NOT NULL DEFAULT 0,
    avg_speed           NUMERIC(8, 2),
    live_fare_exposure  NUMERIC(12, 2) NOT NULL DEFAULT 0,
    event_count         BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zone_id, time_of_day)
);

-- 1-minute tumbling windows (historical trend for the API / dashboard).
CREATE TABLE IF NOT EXISTS fleet_metrics_windows (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    zone_id             TEXT NOT NULL,
    time_of_day         TEXT NOT NULL,
    active_vehicles     INTEGER NOT NULL DEFAULT 0,
    idle_vehicles       INTEGER NOT NULL DEFAULT 0,
    idle_ratio          NUMERIC(8, 4) NOT NULL DEFAULT 0,
    trips_in_window     INTEGER NOT NULL DEFAULT 0,
    trips_per_hour      NUMERIC(12, 2) NOT NULL DEFAULT 0,
    avg_speed           NUMERIC(8, 2),
    live_fare_exposure  NUMERIC(12, 2) NOT NULL DEFAULT 0,
    event_count         BIGINT NOT NULL DEFAULT 0,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, zone_id, time_of_day)
);

CREATE TABLE IF NOT EXISTS vehicle_status_state (
    vehicle_id          TEXT PRIMARY KEY,
    driver_id           TEXT,
    status              TEXT NOT NULL,
    zone_id             TEXT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    idle_since          TIMESTAMPTZ,
    last_event_ts       TIMESTAMPTZ NOT NULL,
    idle_alert_sent     BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS idle_alerts (
    alert_id            BIGSERIAL PRIMARY KEY,
    alert_type          TEXT NOT NULL,
    vehicle_id          TEXT,
    driver_id           TEXT,
    zone_id             TEXT,
    idle_minutes        NUMERIC(10, 2),
    severity            TEXT NOT NULL DEFAULT 'WARNING',
    message             TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_idle_alerts_created
    ON idle_alerts (created_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_health (
    id                  INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    status              TEXT NOT NULL,
    last_batch_id       BIGINT,
    last_input_rows     BIGINT,
    empty_batch_streak  INTEGER NOT NULL DEFAULT 0,
    last_event_ts       TIMESTAMPTZ,
    message             TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO pipeline_health (id, status, message)
VALUES (1, 'STARTING', 'waiting for first streaming batch')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS vehicle_profitability (
    report_date         DATE NOT NULL,
    vehicle_id          TEXT NOT NULL,
    trip_count          INTEGER NOT NULL DEFAULT 0,
    earnings            NUMERIC(12, 2) NOT NULL DEFAULT 0,
    fuel_cost           NUMERIC(12, 2) NOT NULL DEFAULT 0,
    maintenance_cost    NUMERIC(12, 2) NOT NULL DEFAULT 0,
    distance_covered    NUMERIC(12, 2) NOT NULL DEFAULT 0,
    service_flag        BOOLEAN NOT NULL DEFAULT FALSE,
    profit              NUMERIC(12, 2) NOT NULL DEFAULT 0,
    unprofitable        BOOLEAN NOT NULL DEFAULT FALSE,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, vehicle_id)
);
