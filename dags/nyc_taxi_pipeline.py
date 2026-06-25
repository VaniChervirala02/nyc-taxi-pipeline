from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'vani',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi ELT pipeline using dbt',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'dbt']
) as dag:

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='echo "dbt seed would run here"'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='echo "dbt run would run here"'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='echo "dbt test would run here"'
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='echo "dbt snapshot would run here"'
    )

    # task dependencies
    dbt_seed >> dbt_run >> dbt_test >> dbt_snapshot
