from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'vani',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

DBT_ACCOUNT_ID = "271288"
DBT_JOB_ID = "1082994"
DBT_API_TOKEN = Variable.get("DBT_API_TOKEN")

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi ELT pipeline using dbt Cloud',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'dbt']
) as dag:

    # Sensor — check dbt Cloud API is reachable
    wait_for_dbt_api = BashSensor(
        task_id='wait_for_dbt_api',
        bash_command=f"""
            curl -s -o /dev/null -w "%{{http_code}}" \
            -H "Authorization: Token {DBT_API_TOKEN}" \
            "https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/" \
            | grep -q "200"
        """,
        poke_interval=30,
        timeout=300
    )

    # Trigger dbt Cloud job
    trigger_dbt_job = BashOperator(
        task_id='trigger_dbt_build',
        bash_command=f"""
            curl -s -X POST \
            -H "Authorization: Token {DBT_API_TOKEN}"
