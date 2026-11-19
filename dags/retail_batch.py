from datetime import datetime, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


with DAG(
    dag_id="retail_batch",
    schedule="@daily",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
) as dag:
    load_orders = BashOperator(
        task_id="load_orders",
        cwd="/opt/retail",
        bash_command="/home/airflow/retail-venv/bin/python -m src.load_postgres",
    )
    build_reporting = BashOperator(
        task_id="build_reporting",
        bash_command=(
            "/home/airflow/retail-venv/bin/dbt build --project-dir /opt/retail/dbt "
            "--profiles-dir /opt/retail/dbt --target-path /tmp/retail-dbt-target "
            "--log-path /tmp/retail-dbt-logs"
        ),
    )
    load_orders >> build_reporting
