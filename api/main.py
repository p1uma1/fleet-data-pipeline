from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from database import get_connection


app = FastAPI(
    title="Fleet Data Pipeline API",
    description="Serving API for real-time and batch fleet analytics",
    version="1.0.0",
)


@app.get("/")
def root():
    return {
        "message": "Fleet Data Pipeline API",
        "status": "running",
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
    }


@app.get("/db-health")
def database_health():
    connection = None

    try:
        connection = get_connection()

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 AS result;")
            row = cursor.fetchone()

        return {
            "status": "ok",
            "database": "connected",
            "result": row["result"],
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Database connection failed: {exc}",
        )

    finally:
        if connection:
            connection.close()


# ---------------------------------------------------------
# Batch profitability
# ---------------------------------------------------------

@app.get("/profitability")
def get_profitability(
    report_date: Optional[date] = Query(default=None)
):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:

            if report_date:
                cursor.execute(
                    """
                    SELECT
                        report_date,
                        vehicle_id,
                        trip_count,
                        earnings,
                        fuel_cost,
                        maintenance_cost,
                        distance_covered,
                        service_flag,
                        profit,
                        unprofitable,
                        computed_at
                    FROM vehicle_profitability
                    WHERE report_date = %s
                    ORDER BY profit DESC;
                    """,
                    (report_date,),
                )

            else:
                cursor.execute(
                    """
                    SELECT
                        report_date,
                        vehicle_id,
                        trip_count,
                        earnings,
                        fuel_cost,
                        maintenance_cost,
                        distance_covered,
                        service_flag,
                        profit,
                        unprofitable,
                        computed_at
                    FROM vehicle_profitability
                    WHERE report_date = (
                        SELECT MAX(report_date)
                        FROM vehicle_profitability
                    )
                    ORDER BY profit DESC;
                    """
                )

            rows = cursor.fetchall()

        return {
            "count": len(rows),
            "data": rows,
        }

    finally:
        connection.close()


@app.get("/profitability/{vehicle_id}")
def get_vehicle_profitability(
    vehicle_id: str,
    report_date: Optional[date] = Query(default=None),
):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:

            if report_date:
                cursor.execute(
                    """
                    SELECT *
                    FROM vehicle_profitability
                    WHERE vehicle_id = %s
                      AND report_date = %s;
                    """,
                    (vehicle_id, report_date),
                )

            else:
                cursor.execute(
                    """
                    SELECT *
                    FROM vehicle_profitability
                    WHERE vehicle_id = %s
                    ORDER BY report_date DESC
                    LIMIT 1;
                    """,
                    (vehicle_id,),
                )

            row = cursor.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"No profitability data found for {vehicle_id}",
            )

        return row

    finally:
        connection.close()


# ---------------------------------------------------------
# Real-time fleet analytics
# ---------------------------------------------------------

@app.get("/fleet/realtime")
def get_realtime_fleet_metrics():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM fleet_metrics_realtime
                WHERE snapshot_id = 1;
                """
            )

            row = cursor.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail="Realtime fleet metrics not available",
            )

        return row

    finally:
        connection.close()


# ---------------------------------------------------------
# Zone earnings
# ---------------------------------------------------------

@app.get("/zones/earnings")
def get_zone_earnings():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM zone_earnings_realtime
                ORDER BY live_fare_exposure DESC;
                """
            )

            rows = cursor.fetchall()

        return {
            "count": len(rows),
            "data": rows,
        }

    finally:
        connection.close()


# ---------------------------------------------------------
# Vehicle status
# ---------------------------------------------------------

@app.get("/vehicles/status")
def get_vehicle_status():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM vehicle_status_state
                ORDER BY vehicle_id;
                """
            )

            rows = cursor.fetchall()

        return {
            "count": len(rows),
            "data": rows,
        }

    finally:
        connection.close()


# ---------------------------------------------------------
# Idle alerts
# ---------------------------------------------------------

@app.get("/alerts")
def get_alerts(
    limit: int = Query(default=50, ge=1, le=500)
):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM idle_alerts
                ORDER BY created_at DESC
                LIMIT %s;
                """,
                (limit,),
            )

            rows = cursor.fetchall()

        return {
            "count": len(rows),
            "data": rows,
        }

    finally:
        connection.close()


# ---------------------------------------------------------
# Pipeline health
# ---------------------------------------------------------

@app.get("/pipeline/health")
def get_pipeline_health():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM pipeline_health
                WHERE id = 1;
                """
            )

            row = cursor.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail="Pipeline health information not available",
            )

        return row

    finally:
        connection.close()
