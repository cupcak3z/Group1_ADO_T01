from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="orchestrate_s3_to_snowflake",
    default_args=default_args,
    description="Orchestrates the Python script to load S3 data into Snowflake",
    schedule_interval="0 6,14,22 * * *", 
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    run_script = BashOperator(
        task_id="run_python_script",
        bash_command="source /home/astro/dbt_venv/bin/activate && python /home/astro/files/s3_snowflake_incremental_load.py",
    )

