"""Batch layer: join daily vehicle expenses with telemetry earnings.

Person 3's Airflow DAG should spark-submit this job once per simulated day.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STREAMING_ROOT = Path(__file__).resolve().parents[1]
if str(STREAMING_ROOT) not in sys.path:
    sys.path.insert(0, str(STREAMING_ROOT))

from src.config import settings  # noqa: E402
from src.logging_utils import get_logger, log_event  # noqa: E402
from src.profitability import compute_profit  # noqa: E402
from src.sinks import pg_connection  # noqa: E402
from src.transforms import daily_trip_earnings  # noqa: E402

logger = get_logger("batch_profitability", stage="batch")

EXPENSE_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), False),
        StructField("fuel_cost", DoubleType(), True),
        StructField("maintenance_cost", DoubleType(), True),
        StructField("distance_covered", DoubleType(), True),
        StructField("service_flag", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily vehicle profitability reconciliation")
    parser.add_argument(
        "--expenses-path",
        default=settings.expenses_path,
        help="CSV path written by Person 1's daily batch source",
    )
    parser.add_argument(
        "--report-date",
        default=settings.report_date,
        help="Simulated business date YYYY-MM-DD",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("fleet-batch-profitability")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    report_date = date.fromisoformat(args.report_date)
    expenses_path = Path(args.expenses_path)
    if not expenses_path.is_absolute():
        expenses_path = Path(__file__).resolve().parents[2] / expenses_path

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    log_event(
        logger,
        "starting_batch_layer",
        expenses_path=str(expenses_path),
        report_date=str(report_date),
        jdbc=settings.jdbc_url,
    )

    expenses = (
        spark.read.option("header", True)
        .schema(EXPENSE_SCHEMA)
        .csv(str(expenses_path))
        .withColumn("vehicle_id", F.trim(F.col("vehicle_id")))
        .withColumn("fuel_cost", F.coalesce(F.col("fuel_cost"), F.lit(0.0)))
        .withColumn("maintenance_cost", F.coalesce(F.col("maintenance_cost"), F.lit(0.0)))
        .withColumn("distance_covered", F.coalesce(F.col("distance_covered"), F.lit(0.0)))
        .withColumn(
            "service_flag",
            F.lower(F.trim(F.col("service_flag"))).isin("true", "t", "1", "yes", "y"),
        )
    )

    silver = (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", "telemetry_silver")
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .filter(F.to_date(F.col("event_ts")) == F.lit(report_date.isoformat()))
    )

    earnings = daily_trip_earnings(silver)
    joined = expenses.join(earnings, on="vehicle_id", how="left").na.fill(
        {"earnings": 0.0, "trip_count": 0}
    )

    profit_rows = []
    computed_at = datetime.now(timezone.utc)
    for row in joined.collect():
        profit, unprofitable = compute_profit(
            row["earnings"] or 0.0,
            row["fuel_cost"] or 0.0,
            row["maintenance_cost"] or 0.0,
        )
        profit_rows.append(
            {
                "report_date": report_date,
                "vehicle_id": row["vehicle_id"],
                "trip_count": int(row["trip_count"] or 0),
                "earnings": float(row["earnings"] or 0.0),
                "fuel_cost": float(row["fuel_cost"] or 0.0),
                "maintenance_cost": float(row["maintenance_cost"] or 0.0),
                "distance_covered": float(row["distance_covered"] or 0.0),
                "service_flag": bool(row["service_flag"]),
                "profit": profit,
                "unprofitable": unprofitable,
                "computed_at": computed_at,
            }
        )

    result_schema = StructType(
        [
            StructField("report_date", DateType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("trip_count", IntegerType(), False),
            StructField("earnings", DoubleType(), False),
            StructField("fuel_cost", DoubleType(), False),
            StructField("maintenance_cost", DoubleType(), False),
            StructField("distance_covered", DoubleType(), False),
            StructField("service_flag", BooleanType(), False),
            StructField("profit", DoubleType(), False),
            StructField("unprofitable", BooleanType(), False),
            StructField("computed_at", TimestampType(), False),
        ]
    )
    result = spark.createDataFrame(profit_rows, schema=result_schema)

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM vehicle_profitability WHERE report_date = %s",
                (report_date,),
            )
        conn.commit()

    (
        result.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", "vehicle_profitability")
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    unprofitable_count = sum(1 for row in profit_rows if row["unprofitable"])
    log_event(
        logger,
        "profitability_written",
        vehicles=len(profit_rows),
        unprofitable=unprofitable_count,
        report_date=str(report_date),
    )
    result.select(
        "vehicle_id",
        "earnings",
        "fuel_cost",
        "maintenance_cost",
        "profit",
        "unprofitable",
    ).orderBy(F.col("profit").asc()).show(truncate=False)


if __name__ == "__main__":
    main()
