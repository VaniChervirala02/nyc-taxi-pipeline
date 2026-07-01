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

SENSOR_CMD = 'curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Token ' + DBT_API_TOKEN + '" "https://cloud.getdbt.com/api/v2/accounts/' + DBT_ACCOUNT_ID + '/" | grep -q "200"'

TRIGGER_CMD = 'curl -s -X POST -H "Authorization: Token ' + DBT_API_TOKEN + '" -H "Content-Type: application/json" -d \'{"cause": "Triggered by Airflow"}\' "https://cloud.getdbt.com/api/v2/accounts/' + DBT_ACCOUNT_ID + '/jobs/' + DBT_JOB_ID + '/run/"'

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi ELT pipeline using dbt Cloud',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'dbt']
) as dag:

    wait_for_dbt_api = BashSensor(
        task_id='wait_for_dbt_api',
        bash_command=SENSOR_CMD,
        poke_interval=30,
        timeout=300
    )

    trigger_dbt_job = BashOperator(
        task_id='trigger_dbt_build',
        bash_command=TRIGGER_CMD
    )

    wait_for_dbt_api >> trigger_dbt_job
