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
    dag_id="orchestrate_dbt_to_snowflake",
    default_args=default_args,
    description="Orchestrates dbt commands to run and test models in Snowflake",
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
) as dag:

    # Task: Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "source /home/astro/dbt_venv/bin/activate && " 
            "dbt run --project-dir /home/astro/files "  
            "--profiles-dir /home/astro/files" 
        ),
    )

    # Task: Test dbt models
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "source /home/astro/dbt_venv/bin/activate && "  
            "dbt test --project-dir /home/astro/files "  
            "--profiles-dir /home/astro/files" 
        ),
    )

    # Task Dependencies
    dbt_run >> dbt_test
