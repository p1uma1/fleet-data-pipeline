from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def test_airflow():
    print("Fleet Data Pipeline Airflow test successful!")
    print("Airflow scheduler and DAG execution are working.")


with DAG(
    dag_id="fleet_airflow_test",
    description="Simple test DAG for the Fleet Data Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fleet", "test"],
) as dag:

    test_task = PythonOperator(
        task_id="test_airflow_setup",
        python_callable=test_airflow,
    )