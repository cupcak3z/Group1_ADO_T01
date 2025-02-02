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
    dag_id="orchestrate_mysql_to_s3_to_snowflake",
    default_args=default_args,
    description="Orchestrates the Python scripts to load on premise data from MySQL to S3 then into Snowflake",
    schedule_interval="0 22,6,14 * * *",  # Runs at 6am, 2pm, and 10pm SGT
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task: Load data to S3
    run_mysql_to_S3_script = BashOperator(
        task_id="mysql_to_S3",
        bash_command="python /home/astro/files/mysql_s3_incremental_load.py"
    )

    # Task: Load data to snowflake
    run_S3_to_snowflake_script = BashOperator(
        task_id="S3_to_snowflake",
        bash_command="python /home/astro/files/s3_snowflake_incremental_load.py",
    )

    # Task: Test dbt models
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test --project-dir /home/astro/files "
            "--profiles-dir /home/astro/files"
        ),
    )

    # Task: Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run --project-dir /home/astro/files "
            "--profiles-dir /home/astro/files"
        ),
    )

    # Task Dependencies
    run_mysql_to_S3_script >> run_S3_to_snowflake_script >> dbt_test >> dbt_run
