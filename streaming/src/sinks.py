"""JDBC, Postgres upserts, and Kafka alert sinks used by foreachBatch."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession

from src.config import settings
from src.logging_utils import get_logger, log_event
from src.schema import alert_schema

logger = get_logger(__name__, stage="sink")


def _jdbc_options(table: str) -> dict[str, str]:
    return {
        "url": settings.jdbc_url,
        "dbtable": table,
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "driver": "org.postgresql.Driver",
    }


def write_jdbc(df: DataFrame, table: str, mode: str = "append") -> None:
    if df.isEmpty():
        return
    writer = df.write.format("jdbc").options(**_jdbc_options(table)).mode(mode)
    if mode == "overwrite":
        writer = writer.option("truncate", "true")
    writer.save()
    log_event(logger, "jdbc_write", table=table, mode=mode, rows=df.count())


def pg_connection():
    return psycopg2.connect(settings.database_url)


def upsert_fleet_snapshot(row: dict[str, Any] | None) -> None:
    if not row:
        return
    sql = """
        INSERT INTO fleet_metrics_realtime (
            snapshot_id, window_start, window_end, active_vehicles, idle_vehicles,
            total_vehicles, idle_ratio, trips_in_window, trips_per_hour,
            avg_speed, live_fare_exposure, event_count, updated_at
        ) VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (snapshot_id) DO UPDATE SET
            window_start = EXCLUDED.window_start,
            window_end = EXCLUDED.window_end,
            active_vehicles = EXCLUDED.active_vehicles,
            idle_vehicles = EXCLUDED.idle_vehicles,
            total_vehicles = EXCLUDED.total_vehicles,
            idle_ratio = EXCLUDED.idle_ratio,
            trips_in_window = EXCLUDED.trips_in_window,
            trips_per_hour = EXCLUDED.trips_per_hour,
            avg_speed = EXCLUDED.avg_speed,
            live_fare_exposure = EXCLUDED.live_fare_exposure,
            event_count = EXCLUDED.event_count,
            updated_at = NOW()
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    row.get("window_start"),
                    row.get("window_end"),
                    int(row.get("active_vehicles") or 0),
                    int(row.get("idle_vehicles") or 0),
                    int(row.get("total_vehicles") or 0),
                    float(row.get("idle_ratio") or 0),
                    int(row.get("trips_in_window") or 0),
                    float(row.get("trips_per_hour") or 0),
                    row.get("avg_speed"),
                    float(row.get("live_fare_exposure") or 0),
                    int(row.get("event_count") or 0),
                ),
            )
        conn.commit()


def replace_zone_snapshot(rows: list[dict[str, Any]]) -> None:
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE zone_earnings_realtime")
            if rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO zone_earnings_realtime (
                        zone_id, time_of_day, window_start, window_end,
                        active_vehicles, trips_in_window, avg_speed,
                        live_fare_exposure, event_count, updated_at
                    ) VALUES %s
                    """,
                    [
                        (
                            r["zone_id"],
                            r["time_of_day"],
                            r.get("window_start"),
                            r.get("window_end"),
                            int(r.get("active_vehicles") or 0),
                            int(r.get("trips_in_window") or 0),
                            r.get("avg_speed"),
                            float(r.get("live_fare_exposure") or 0),
                            int(r.get("event_count") or 0),
                            datetime.now(timezone.utc),
                        )
                        for r in rows
                    ],
                )
        conn.commit()


def upsert_window_metrics(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO fleet_metrics_windows (
            window_start, window_end, zone_id, time_of_day, active_vehicles,
            idle_vehicles, idle_ratio, trips_in_window, trips_per_hour,
            avg_speed, live_fare_exposure, event_count, computed_at
        ) VALUES %s
        ON CONFLICT (window_start, zone_id, time_of_day) DO UPDATE SET
            window_end = EXCLUDED.window_end,
            active_vehicles = EXCLUDED.active_vehicles,
            idle_vehicles = EXCLUDED.idle_vehicles,
            idle_ratio = EXCLUDED.idle_ratio,
            trips_in_window = EXCLUDED.trips_in_window,
            trips_per_hour = EXCLUDED.trips_per_hour,
            avg_speed = EXCLUDED.avg_speed,
            live_fare_exposure = EXCLUDED.live_fare_exposure,
            event_count = EXCLUDED.event_count,
            computed_at = NOW()
    """
    values = [
        (
            r["window_start"],
            r["window_end"],
            r["zone_id"],
            r["time_of_day"],
            int(r.get("active_vehicles") or 0),
            int(r.get("idle_vehicles") or 0),
            float(r.get("idle_ratio") or 0),
            int(r.get("trips_in_window") or 0),
            float(r.get("trips_per_hour") or 0),
            r.get("avg_speed"),
            float(r.get("live_fare_exposure") or 0),
            int(r.get("event_count") or 0),
            datetime.now(timezone.utc),
        )
        for r in rows
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()


def load_vehicle_state(vehicle_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
    ids = list(vehicle_ids)
    if not ids:
        return {}
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vehicle_id, driver_id, status, idle_since,
                       last_event_ts, idle_alert_sent
                FROM vehicle_status_state
                WHERE vehicle_id = ANY(%s)
                """,
                (ids,),
            )
            rows = cur.fetchall()
    return {
        row[0]: {
            "vehicle_id": row[0],
            "driver_id": row[1],
            "status": row[2],
            "idle_since": row[3],
            "last_event_ts": row[4],
            "idle_alert_sent": row[5],
        }
        for row in rows
    }


def upsert_vehicle_states(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO vehicle_status_state (
            vehicle_id, driver_id, status, zone_id, lat, lon,
            idle_since, last_event_ts, idle_alert_sent, updated_at
        ) VALUES %s
        ON CONFLICT (vehicle_id) DO UPDATE SET
            driver_id = EXCLUDED.driver_id,
            status = EXCLUDED.status,
            zone_id = EXCLUDED.zone_id,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            idle_since = EXCLUDED.idle_since,
            last_event_ts = EXCLUDED.last_event_ts,
            idle_alert_sent = EXCLUDED.idle_alert_sent,
            updated_at = NOW()
    """
    values = [
        (
            r["vehicle_id"],
            r.get("driver_id"),
            r["status"],
            r.get("zone_id"),
            r.get("lat"),
            r.get("lon"),
            r.get("idle_since"),
            r["last_event_ts"],
            bool(r.get("idle_alert_sent", False)),
            datetime.now(timezone.utc),
        )
        for r in rows
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()


def insert_alerts(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO idle_alerts (
            alert_type, vehicle_id, driver_id, zone_id,
            idle_minutes, severity, message, created_at
        ) VALUES %s
    """
    values = [
        (
            r["alert_type"],
            r.get("vehicle_id"),
            r.get("driver_id"),
            r.get("zone_id"),
            r.get("idle_minutes"),
            r.get("severity", "WARNING"),
            r["message"],
            r.get("created_at") or datetime.now(timezone.utc),
        )
        for r in rows
    ]
    with pg_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()


def upsert_pipeline_health(
    status: str,
    batch_id: int,
    input_rows: int,
    empty_streak: int,
    last_event_ts: datetime | None,
    message: str,
) -> None:
    sql = """
        INSERT INTO pipeline_health (
            id, status, last_batch_id, last_input_rows, empty_batch_streak,
            last_event_ts, message, updated_at
        ) VALUES (1, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            last_batch_id = EXCLUDED.last_batch_id,
            last_input_rows = EXCLUDED.last_input_rows,
            empty_batch_streak = EXCLUDED.empty_batch_streak,
            last_event_ts = EXCLUDED.last_event_ts,
            message = EXCLUDED.message,
            updated_at = NOW()
    """
    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (status, batch_id, input_rows, empty_streak, last_event_ts, message),
            )
        conn.commit()


def publish_alerts_to_kafka(spark: SparkSession, alerts: list[dict[str, Any]]) -> None:
    if not alerts:
        return
    df = spark.createDataFrame(alerts, schema=alert_schema)
    (
        df.selectExpr(
            "CAST(coalesce(vehicle_id, 'pipeline') AS STRING) AS key",
            "to_json(struct(*)) AS value",
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("topic", settings.alerts_topic)
        .save()
    )
    log_event(logger, "kafka_alerts_written", count=len(alerts), topic=settings.alerts_topic)
