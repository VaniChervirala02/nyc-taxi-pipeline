from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'vani',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi ELT pipeline using dbt Cloud',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'dbt']
) as dag:

    trigger_dbt_job = DbtCloudRunJobOperator(
        task_id='trigger_dbt_build',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=1082994,
        check_interval=10,
        timeout=300
    )
