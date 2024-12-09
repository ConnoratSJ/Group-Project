from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook

DBT_PROJECT_DIR = "/opt/airflow/dbt"

# Fetch the connection details from Airflow's connection manager
conn = BaseHook.get_connection('my_snowflake_conn')

# Define the default arguments for the DAG including environment variables setup
default_args = {
    "env": {
        "DBT_USER": conn.login,
        "DBT_PASSWORD": conn.password,
        "DBT_ACCOUNT": conn.extra_dejson.get("account"),
        "DBT_SCHEMA": conn.schema,
        "DBT_DATABASE": conn.extra_dejson.get("database"),
        "DBT_ROLE": conn.extra_dejson.get("role"),
        "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
        "DBT_TYPE": "snowflake",
        # Ensure the dbt executable path is correct for your environment
        "PATH": "/home/airflow/.local/bin:$PATH"
    }
}

with DAG(
    "dbt_run",
    start_date=datetime(2024, 10, 14),
    description="A sample Airflow DAG to invoke dbt runs using BashOperator",
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    # Define task dependencies
    dbt_run >> dbt_test >> dbt_snapshot
