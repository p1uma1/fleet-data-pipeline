from datetime import datetime
from pathlib import Path
import csv

from airflow import DAG
from airflow.operators.python import PythonOperator

REPORT_DATE = "2026-09-27"
EXPENSE_FILE = "/opt/project/data/batch/expenses_20260927.csv.example"
SPARK_CONTAINER = "fleet-spark"


def check_expense_file():
    path = Path(EXPENSE_FILE)

    if not path.exists():
        raise FileNotFoundError(f"Expense CSV not found: {EXPENSE_FILE}")

    print(f"Expense file found: {EXPENSE_FILE}")


def validate_expense_csv():
    required_columns = {
        "vehicle_id",
        "fuel_cost",
        "maintenance_cost",
        "distance_covered",
        "service_flag",
    }

    with open(EXPENSE_FILE, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError("Expense CSV has no header")

        actual_columns = set(reader.fieldnames)
        missing_columns = required_columns - actual_columns

        if missing_columns:
            raise ValueError(
                f"Missing columns: {sorted(missing_columns)}"
            )

        rows = list(reader)

    if not rows:
        raise ValueError("Expense CSV contains no rows")

    print(f"CSV validation successful. Rows found: {len(rows)}")


def run_spark_profitability():
    import docker

    client = docker.from_env()

    container = client.containers.get(SPARK_CONTAINER)

    command = [
        "bash",
        "-lc",
        """
        mkdir -p /tmp/spark-ivy &&

        /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/spark-ivy \
          --packages org.postgresql:postgresql:42.7.4 \
          /opt/project/streaming/jobs/batch_profitability.py \
          --expenses-path /opt/project/data/batch/expenses_20260927.csv.example \
          --report-date 2026-09-27
        """,
    ]

    print("Starting Spark profitability job...")

    result = container.exec_run(
        command,
        stdout=True,
        stderr=True,
        demux=False,
    )

    output = result.output.decode(
        "utf-8",
        errors="replace",
    )

    print(output)

    if result.exit_code != 0:
        raise RuntimeError(
            f"Spark job failed with exit code {result.exit_code}"
        )

    print("Spark profitability job completed successfully.")


def verify_profitability_rows():
    import psycopg2

    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        database="fleet",
        user="fleet",
        password="fleet",
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM vehicle_profitability
                WHERE report_date = %s;
                """,
                (REPORT_DATE,),
            )

            count = cursor.fetchone()[0]

        print(
            f"vehicle_profitability rows for "
            f"{REPORT_DATE}: {count}"
        )

        if count == 0:
            raise ValueError(
                "No profitability rows were written."
            )

    finally:
        connection.close()


with DAG(
    dag_id="fleet_profitability_pipeline",
    description="Daily fleet profitability batch pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fleet", "batch", "spark"],
) as dag:

    check_file = PythonOperator(
        task_id="check_expense_file",
        python_callable=check_expense_file,
    )

    validate_csv = PythonOperator(
        task_id="validate_expense_csv",
        python_callable=validate_expense_csv,
    )

    run_spark = PythonOperator(
        task_id="run_spark_profitability",
        python_callable=run_spark_profitability,
    )

    verify_rows = PythonOperator(
        task_id="verify_profitability_rows",
        python_callable=verify_profitability_rows,
    )

    check_file >> validate_csv >> run_spark >> verify_rows
