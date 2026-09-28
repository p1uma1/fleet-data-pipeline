"""Cleaning, enrichment, window aggregations, and profitability math."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

from src.events import VALID_STATUSES
from src.profitability import compute_profit
from src.schema import telemetry_schema
from src.zones import MAX_LAT, MAX_LON, MIN_LAT, MIN_LON, GRID_SIZE

__all__ = [
    "parse_telemetry",
    "clean_and_enrich",
    "latest_per_vehicle",
    "snapshot_fleet_metrics",
    "snapshot_zone_earnings",
    "windowed_zone_metrics",
    "daily_trip_earnings",
    "compute_profit",
]


def parse_telemetry(raw: DataFrame) -> DataFrame:
    """Parse Kafka JSON values into typed columns."""
    json_col = F.col("value").cast("string")
    parsed = raw.select(
        F.from_json(json_col, telemetry_schema).alias("event"),
        F.col("timestamp").alias("kafka_ts"),
    ).select("event.*", "kafka_ts")
    return parsed


def clean_and_enrich(events: DataFrame) -> DataFrame:
    """Drop poison rows, clamp GPS to Colombo, add zone and time-of-day."""
    lat_span = (MAX_LAT - MIN_LAT) / GRID_SIZE
    lon_span = (MAX_LON - MIN_LON) / GRID_SIZE

    cleaned = (
        events.withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("vehicle_id", F.trim(F.col("vehicle_id")))
        .withColumn("driver_id", F.trim(F.col("driver_id")))
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        .filter(F.col("vehicle_id").isNotNull() & (F.col("vehicle_id") != ""))
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("status").isin(list(VALID_STATUSES)))
        .withColumn("lat", F.col("lat").cast(DoubleType()))
        .withColumn("lon", F.col("lon").cast(DoubleType()))
        .withColumn("speed", F.coalesce(F.col("speed").cast(DoubleType()), F.lit(0.0)))
        .withColumn("fare", F.coalesce(F.col("fare").cast(DoubleType()), F.lit(0.0)))
        .withColumn("lat", F.when(F.col("lat") < F.lit(MIN_LAT), F.lit(MIN_LAT)).otherwise(F.col("lat")))
        .withColumn("lat", F.when(F.col("lat") > F.lit(MAX_LAT), F.lit(MAX_LAT)).otherwise(F.col("lat")))
        .withColumn("lon", F.when(F.col("lon") < F.lit(MIN_LON), F.lit(MIN_LON)).otherwise(F.col("lon")))
        .withColumn("lon", F.when(F.col("lon") > F.lit(MAX_LON), F.lit(MAX_LON)).otherwise(F.col("lon")))
    )

    zone_row = F.least(
        F.greatest(F.floor((F.col("lat") - F.lit(MIN_LAT)) / F.lit(lat_span)).cast(IntegerType()), F.lit(0)),
        F.lit(GRID_SIZE - 1),
    )
    zone_col = F.least(
        F.greatest(F.floor((F.col("lon") - F.lit(MIN_LON)) / F.lit(lon_span)).cast(IntegerType()), F.lit(0)),
        F.lit(GRID_SIZE - 1),
    )

    hour = F.hour(F.col("event_ts"))
    time_of_day = (
        F.when((hour >= 5) & (hour < 12), F.lit("morning"))
        .when((hour >= 12) & (hour < 17), F.lit("afternoon"))
        .when((hour >= 17) & (hour < 21), F.lit("evening"))
        .otherwise(F.lit("night"))
    )

    return cleaned.withColumn(
        "zone_id",
        F.concat(F.lit("Z"), zone_row.cast(StringType()), zone_col.cast(StringType())),
    ).withColumn("time_of_day", time_of_day)


def latest_per_vehicle(events: DataFrame) -> DataFrame:
    from pyspark.sql.window import Window

    window = Window.partitionBy("vehicle_id").orderBy(F.col("event_ts").desc())
    return (
        events.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def snapshot_fleet_metrics(events: DataFrame, window_seconds: int = 10) -> DataFrame:
    """Right-now KPIs from the current micro-batch."""
    latest = latest_per_vehicle(events)
    per_trip = (
        events.filter(F.col("trip_id").isNotNull())
        .groupBy("trip_id")
        .agg(F.max("fare").alias("trip_fare"))
    )

    totals = latest.agg(
        F.countDistinct("vehicle_id").alias("total_vehicles"),
        F.sum(F.when(F.col("status") != F.lit("idle"), 1).otherwise(0)).alias("active_vehicles"),
        F.sum(F.when(F.col("status") == F.lit("idle"), 1).otherwise(0)).alias("idle_vehicles"),
        F.avg("speed").alias("avg_speed"),
        F.count(F.lit(1)).alias("event_count"),
        F.min("event_ts").alias("window_start"),
        F.max("event_ts").alias("window_end"),
    )

    trip_stats = events.agg(
        F.countDistinct(
            F.when(F.col("status") == F.lit("on_trip"), F.col("trip_id"))
        ).alias("trips_in_window")
    )

    fare_stats = per_trip.agg(
        F.coalesce(F.sum("trip_fare"), F.lit(0.0)).alias("live_fare_exposure")
    )

    return (
        totals.crossJoin(trip_stats)
        .crossJoin(fare_stats)
        .withColumn(
            "idle_ratio",
            F.when(
                F.col("total_vehicles") > 0,
                F.col("idle_vehicles") / F.col("total_vehicles"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "trips_per_hour",
            F.col("trips_in_window") * F.lit(3600.0 / window_seconds),
        )
    )


def snapshot_zone_earnings(events: DataFrame) -> DataFrame:
    latest = latest_per_vehicle(events)
    per_trip_zone = (
        events.filter(F.col("trip_id").isNotNull())
        .groupBy("trip_id", "zone_id", "time_of_day")
        .agg(F.max("fare").alias("trip_fare"))
    )

    vehicle_stats = latest.groupBy("zone_id", "time_of_day").agg(
        F.sum(F.when(F.col("status") != F.lit("idle"), 1).otherwise(0)).alias("active_vehicles"),
        F.avg("speed").alias("avg_speed"),
        F.count(F.lit(1)).alias("event_count"),
        F.min("event_ts").alias("window_start"),
        F.max("event_ts").alias("window_end"),
    )

    trip_stats = events.groupBy("zone_id", "time_of_day").agg(
        F.countDistinct(
            F.when(F.col("status") == F.lit("on_trip"), F.col("trip_id"))
        ).alias("trips_in_window")
    )

    fare_stats = per_trip_zone.groupBy("zone_id", "time_of_day").agg(
        F.coalesce(F.sum("trip_fare"), F.lit(0.0)).alias("live_fare_exposure")
    )

    return (
        vehicle_stats.join(trip_stats, ["zone_id", "time_of_day"], "left")
        .join(fare_stats, ["zone_id", "time_of_day"], "left")
        .na.fill({"trips_in_window": 0, "live_fare_exposure": 0.0})
    )


def windowed_zone_metrics(events: DataFrame, watermark: str, window: str) -> DataFrame:
    """1-minute tumbling windows with watermark — Spark Structured Streaming aggregation."""
    return (
        events.withWatermark("event_ts", watermark)
        .groupBy(
            F.window(F.col("event_ts"), window),
            F.col("zone_id"),
            F.col("time_of_day"),
        )
        .agg(
            F.countDistinct("vehicle_id").alias("total_vehicles"),
            F.countDistinct(
                F.when(F.col("status") != F.lit("idle"), F.col("vehicle_id"))
            ).alias("active_vehicles"),
            F.countDistinct(
                F.when(F.col("status") == F.lit("idle"), F.col("vehicle_id"))
            ).alias("idle_vehicles"),
            F.countDistinct(
                F.when(F.col("status") == F.lit("on_trip"), F.col("trip_id"))
            ).alias("trips_in_window"),
            F.avg("speed").alias("avg_speed"),
            F.max("fare").alias("live_fare_exposure"),
            F.count(F.lit(1)).alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone_id",
            "time_of_day",
            "active_vehicles",
            "idle_vehicles",
            F.when(
                F.col("total_vehicles") > 0,
                F.col("idle_vehicles") / F.col("total_vehicles"),
            )
            .otherwise(F.lit(0.0))
            .alias("idle_ratio"),
            "trips_in_window",
            (F.col("trips_in_window") * F.lit(60.0)).alias("trips_per_hour"),
            "avg_speed",
            F.coalesce(F.col("live_fare_exposure"), F.lit(0.0)).alias("live_fare_exposure"),
            "event_count",
        )
    )


def daily_trip_earnings(silver: DataFrame) -> DataFrame:
    """Fare is cumulative per trip in Person 1's simulator, so take max(fare) per trip."""
    return (
        silver.filter(F.col("trip_id").isNotNull() & (F.col("trip_id") != ""))
        .groupBy("vehicle_id", "trip_id")
        .agg(F.max("fare").alias("trip_fare"))
        .groupBy("vehicle_id")
        .agg(
            F.sum("trip_fare").alias("earnings"),
            F.count("trip_id").alias("trip_count"),
        )
    )
