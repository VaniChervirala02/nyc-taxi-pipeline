from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
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

    # Sensor — wait for source table to exist
    wait_for_source = BigQueryTableExistenceSensor(
        task_id='wait_for_source_table',
        project_id='confident-totem-458819-b7',
        dataset_id='new_york_taxi_trips',
        table_id='tlc_yellow_trips_2022',
        gcp_conn_id='bigquery_default',
        poke_interval=30,
        timeout=300
    )

    # Trigger dbt Cloud job
    trigger_dbt_job = BashOperator(
        task_id='trigger_dbt_build',
        bash_command=f"""
            curl -s -X POST \
            -H "Authorization: Token {DBT_API_TOKEN}" \
            -H "Content-Type: application/json" \
            -d '{{"cause": "Triggered by Airflow"}}' \
            "https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
        """
    )

    # Dependencies
    wait_for_source >> trigger_dbt_job
