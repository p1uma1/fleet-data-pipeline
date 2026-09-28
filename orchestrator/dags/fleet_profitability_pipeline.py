from datetime import datetime
from pathlib import Path
import csv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context


SPARK_CONTAINER = "fleet-spark"
BATCH_DATA_DIR = "/opt/project/data/batch"


def get_run_config():
    context = get_current_context()

    dag_run = context.get("dag_run")

    # Allow manual demo runs to specify a simulated date.
    if dag_run and dag_run.conf and dag_run.conf.get("report_date"):
        report_date = dag_run.conf["report_date"]
    else:
        # Scheduled runs use the Airflow logical date.
        report_date = context["ds"]

    date_compact = report_date.replace("-", "")

    # Prefer a real CSV if available.
    csv_path = Path(
        f"{BATCH_DATA_DIR}/expenses_{date_compact}.csv"
    )

    # Support the current example file during development/demo.
    example_path = Path(
        f"{BATCH_DATA_DIR}/expenses_{date_compact}.csv.example"
    )

    if csv_path.exists():
        expense_file = str(csv_path)
    elif example_path.exists():
        expense_file = str(example_path)
    else:
        expense_file = str(csv_path)

    return report_date, expense_file


def check_expense_file():
    report_date, expense_file = get_run_config()

    path = Path(expense_file)

    if not path.exists():
        raise FileNotFoundError(
            f"No expense CSV found for {report_date}: {expense_file}"
        )

    print(f"Report date: {report_date}")
    print(f"Expense file found: {expense_file}")

    return {
        "report_date": report_date,
        "expense_file": expense_file,
    }


def validate_expense_csv():
    _, expense_file = get_run_config()

    required_columns = {
        "vehicle_id",
        "fuel_cost",
        "maintenance_cost",
        "distance_covered",
        "service_flag",
    }

    with open(
        expense_file,
        newline="",
        encoding="utf-8",
    ) as file:

        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError("Expense CSV has no header")

        actual_columns = set(reader.fieldnames)

        missing_columns = required_columns - actual_columns

        if missing_columns:
            raise ValueError(
                f"Missing required columns: "
                f"{sorted(missing_columns)}"
            )

        rows = list(reader)

    if not rows:
        raise ValueError("Expense CSV contains no data rows")

    print(f"Expense CSV validation successful")
    print(f"Rows found: {len(rows)}")


def run_spark_profitability():
    import docker

    report_date, expense_file = get_run_config()

    client = docker.from_env()

    container = client.containers.get(
        SPARK_CONTAINER
    )

    command = [
        "bash",
        "-lc",
        f"""
        mkdir -p /tmp/spark-ivy &&

        /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/spark-ivy \
          --packages org.postgresql:postgresql:42.7.4 \
          /opt/project/streaming/jobs/batch_profitability.py \
          --expenses-path {expense_file} \
          --report-date {report_date}
        """,
    ]

    print(
        f"Starting profitability batch for "
        f"{report_date}"
    )

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
            f"Spark profitability job failed "
            f"with exit code {result.exit_code}"
        )

    print(
        "Spark profitability batch completed successfully"
    )


def verify_profitability_rows():
    import psycopg2

    report_date, _ = get_run_config()

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
                (report_date,),
            )

            count = cursor.fetchone()[0]

        print(
            f"vehicle_profitability rows "
            f"for {report_date}: {count}"
        )

        if count == 0:
            raise ValueError(
                "Spark finished successfully but "
                "no profitability rows were written."
            )

    finally:
        connection.close()


with DAG(
    dag_id="fleet_profitability_pipeline",
    description=(
        "Daily reconciliation of vehicle expenses "
        "with telemetry earnings"
    ),

    start_date=datetime(2026, 1, 1),

    # Once per simulated/business day.
    schedule="@daily",

    catchup=False,

    tags=[
        "fleet",
        "batch",
        "spark",
        "profitability",
    ],
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

    (
        check_file
        >> validate_csv
        >> run_spark
        >> verify_rows
    )
