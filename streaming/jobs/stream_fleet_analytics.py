"""Spark Structured Streaming speed layer for ride-hailing fleet operations.

Reads Person 1's Kafka telemetry, computes live utilization / earnings by

zone and time-of-day, and emits idle + no-data alerts.

"""

from __future__ import annotations

import sys

from datetime import datetime, timezone

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

STREAMING_ROOT = Path(__file__).resolve().parents[1]

if str(STREAMING_ROOT) not in sys.path:

    sys.path.insert(0, str(STREAMING_ROOT))

from src.alerts import (  # noqa: E402

    is_no_data_alert,

    next_idle_since,

    should_emit_idle_alert,

    should_reset_idle_alert,

    idle_minutes,

)

from src.config import settings  # noqa: E402

from src.logging_utils import get_logger, log_event  # noqa: E402

from src.observability import FleetQueryListener  # noqa: E402

from src.sinks import (  # noqa: E402

    insert_alerts,

    load_vehicle_state,

    publish_alerts_to_kafka,

    replace_zone_snapshot,

    upsert_fleet_snapshot,

    upsert_pipeline_health,

    upsert_vehicle_states,

    upsert_window_metrics,

    write_jdbc,

)

from src.transforms import (  # noqa: E402

    clean_and_enrich,

    latest_per_vehicle,

    parse_telemetry,

    snapshot_fleet_metrics,

    snapshot_zone_earnings,

    windowed_zone_metrics,

)

logger = get_logger("stream_fleet_analytics", stage="streaming")



# Driver-side streak for the NO_DATA health rule.

_empty_batch_streak = 0

_no_data_alert_sent = False

def build_spark(app_name: str) -> SparkSession:

    return (

        SparkSession.builder.appName(app_name)

        .config("spark.sql.session.timeZone", "UTC")

        .config("spark.sql.shuffle.partitions", "4")

        .config("spark.ui.showConsoleProgress", "false")

        .getOrCreate()

    )

def telemetry_stream(spark: SparkSession) -> DataFrame:

    return (

        spark.readStream.format("kafka")

        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)

        .option("subscribe", settings.telemetry_topic)

        .option("startingOffsets", settings.kafka_starting_offsets)

        .option("failOnDataLoss", "false")

        .load()

    )

def _row_to_dict(row) -> dict:

    return row.asDict() if hasattr(row, "asDict") else dict(row)

def _trigger_seconds() -> int:

    token = settings.trigger_interval.strip().split()

    try:

        value = int(token[0])

    except (ValueError, IndexError):

        return 10

    unit = token[1].lower() if len(token) > 1 else "seconds"

    if unit.startswith("minute"):

        return value * 60

    return value

def process_event_batch(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:

    global _empty_batch_streak, _no_data_alert_sent

    alerts: list[dict] = []

    now = datetime.now(timezone.utc)

    if batch_df.isEmpty():

        _empty_batch_streak += 1

        no_data = is_no_data_alert(_empty_batch_streak, settings.no_data_threshold_batches)

        status = "NO_DATA" if no_data else "WAITING"

        message = (

            f"no telemetry for {_empty_batch_streak} consecutive batches"

            if no_data

            else f"empty batch streak={_empty_batch_streak}"

        )

        upsert_pipeline_health(

            status=status,

            batch_id=batch_id,

            input_rows=0,

            empty_streak=_empty_batch_streak,

            last_event_ts=None,

            message=message,

        )

        if no_data and not _no_data_alert_sent:

            alert = {

                "alert_type": "NO_DATA",

                "vehicle_id": None,

                "driver_id": None,

                "zone_id": None,

                "idle_minutes": None,

                "severity": "CRITICAL",

                "message": message,

                "created_at": now.isoformat(),

            }

            insert_alerts([alert])

            publish_alerts_to_kafka(spark, [alert])

            _no_data_alert_sent = True

            log_event(logger, "no_data_alert", streak=_empty_batch_streak, batch_id=batch_id)

        else:

            log_event(logger, "empty_batch", streak=_empty_batch_streak, batch_id=batch_id)

        return

    _empty_batch_streak = 0

    _no_data_alert_sent = False

    cleaned = clean_and_enrich(parse_telemetry(batch_df))

    if cleaned.isEmpty():

        upsert_pipeline_health(

            status="PARSE_EMPTY",

            batch_id=batch_id,

            input_rows=0,

            empty_streak=0,

            last_event_ts=None,

            message="kafka messages did not parse into valid telemetry",

        )

        return

    cleaned.cache()

    input_rows = cleaned.count()

    silver = cleaned.select(

        "trip_id",

        "driver_id",

        "vehicle_id",

        "lat",

        "lon",

        "speed",

        "status",

        "fare",

        "event_ts",

        "zone_id",

        "time_of_day",

    )

    write_jdbc(silver, "telemetry_silver", mode="append")

    snapshot = snapshot_fleet_metrics(cleaned, window_seconds=_trigger_seconds())

    snapshot_rows = [_row_to_dict(r) for r in snapshot.collect()]

    upsert_fleet_snapshot(snapshot_rows[0] if snapshot_rows else None)

    zone_rows = [_row_to_dict(r) for r in snapshot_zone_earnings(cleaned).collect()]

    replace_zone_snapshot(zone_rows)

    latest_rows = [_row_to_dict(r) for r in latest_per_vehicle(cleaned).collect()]

    previous = load_vehicle_state([r["vehicle_id"] for r in latest_rows])

    next_states = []

    for row in latest_rows:

        vehicle_id = row["vehicle_id"]

        prev = previous.get(vehicle_id, {})

        new_status = row["status"]

        event_ts = row["event_ts"]

        idle_since = next_idle_since(

            prev.get("status"), prev.get("idle_since"), new_status, event_ts

        )

        already_sent = bool(prev.get("idle_alert_sent")) and not should_reset_idle_alert(

            new_status

        )

        emit = should_emit_idle_alert(

            idle_since,

            event_ts,

            already_sent,

            threshold_minutes=settings.idle_threshold_minutes,

        )

        minutes = idle_minutes(idle_since, event_ts)

        if emit:

            alert = {

                "alert_type": "IDLE_VEHICLE",

                "vehicle_id": vehicle_id,

                "driver_id": row.get("driver_id"),

                "zone_id": row.get("zone_id"),

                "idle_minutes": round(minutes, 2),

                "severity": "WARNING",

                "message": (

                    f"{vehicle_id} idle for {minutes:.1f} minutes "

                    f"(threshold {settings.idle_threshold_minutes:.0f}m)"

                ),

                "created_at": now.isoformat(),

            }

            alerts.append(alert)

            already_sent = True

        next_states.append(

            {

                "vehicle_id": vehicle_id,

                "driver_id": row.get("driver_id"),

                "status": new_status,

                "zone_id": row.get("zone_id"),

                "lat": row.get("lat"),

                "lon": row.get("lon"),

                "idle_since": idle_since,

                "last_event_ts": event_ts,

                "idle_alert_sent": already_sent,

            }

        )

    upsert_vehicle_states(next_states)

    if alerts:

        insert_alerts(alerts)

        publish_alerts_to_kafka(spark, alerts)

    last_event_ts = max(r["event_ts"] for r in latest_rows)

    upsert_pipeline_health(

        status="HEALTHY",

        batch_id=batch_id,

        input_rows=input_rows,

        empty_streak=0,

        last_event_ts=last_event_ts,

        message="streaming batch processed",

    )

    log_event(

        logger,

        "batch_processed",

        batch_id=batch_id,

        input_rows=input_rows,

        idle_alerts=len(alerts),

        zones=len(zone_rows),

    )

    cleaned.unpersist()

def process_window_batch(_spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:

    if batch_df.isEmpty():

        return

    rows = [_row_to_dict(r) for r in batch_df.collect()]

    upsert_window_metrics(rows)

    log_event(logger, "window_metrics_written", batch_id=batch_id, rows=len(rows))


def main() -> None:

    spark = build_spark(settings.spark_app_name)

    spark.sparkContext.setLogLevel("WARN")

    spark.streams.addListener(FleetQueryListener())

    log_event(

        logger,

        "starting_speed_layer",

        kafka=settings.kafka_bootstrap,

        topic=settings.telemetry_topic,

        jdbc=settings.jdbc_url,

        idle_threshold_minutes=settings.idle_threshold_minutes,

    )

    raw_events = telemetry_stream(spark)

    checkpoint_root = Path(settings.checkpoint_dir)

    events_query = (

        raw_events.writeStream.foreachBatch(

            lambda df, epoch_id: process_event_batch(spark, df, epoch_id)

        )

        .option("checkpointLocation", str(checkpoint_root / "stream_events"))

        .trigger(processingTime=settings.trigger_interval)

        .queryName("fleet_events_sink")

        .start()

    )

    windowed = windowed_zone_metrics(

        clean_and_enrich(parse_telemetry(telemetry_stream(spark))),

        watermark=settings.watermark,

        window=settings.metrics_window,

    )

    windows_query = (

        windowed.writeStream.outputMode("update")

        .foreachBatch(lambda df, epoch_id: process_window_batch(spark, df, epoch_id))

        .option("checkpointLocation", str(checkpoint_root / "stream_windows"))

        .trigger(processingTime=settings.trigger_interval)

        .queryName("fleet_window_metrics")

        .start()

    )

    spark.streams.awaitAnyTermination()

    events_query.stop()

    windows_query.stop()

if __name__ == "__main__":

    main()
