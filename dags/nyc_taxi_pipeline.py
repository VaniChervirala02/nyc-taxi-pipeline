from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'vani',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['chervirala.vani123@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

DBT_ACCOUNT_ID = "271288"
DBT_BUILD_JOB_ID = "1082994"
DBT_TEST_JOB_ID = "1084237"
DBT_API_TOKEN = Variable.get("DBT_API_TOKEN")

SENSOR_CMD = 'RESPONSE=$(curl -s -w "\\n%{http_code}" -H "Authorization: Token ' + DBT_API_TOKEN + '" "https://cloud.getdbt.com/api/v2/accounts/' + DBT_ACCOUNT_ID + '/") && echo "$RESPONSE" && echo "$RESPONSE" | tail -1 | grep -q "200"'

BUILD_CMD = 'RUN_ID=$(curl -s -X POST -H "Authorization: Token ' + DBT_API_TOKEN + '" -H "Content-Type: application/json" -d \'{"cause": "Triggered by Airflow"}\' "https://cloud.getdbt.com/api/v2/accounts/' + DBT_ACCOUNT_ID + '/jobs/' + DBT_BUILD_JOB_ID + '/run/" | python3 -c "import sys,json; print(json.load(sys.stdin)[\'data\'][\'id\'])") && echo "Run ID: $RUN_ID" && while true; do STATUS=$(curl -s -H "Authorization: Token ' + DBT_API_TOKEN + '" "https://cloud.getdbt.com/api/v2/accounts/' + DBT_ACCOUNT_ID + '/runs/$RUN_ID/" | python3 -c "import sys,json; d=json.load(sys.stdin)[\'data\']; print(d[\'status\'])"); echo "Status: $STATUS"; if [ "$STATUS" = "10" ]; then echo "Success!"; exit 0; elif [ "$STATUS" = "20" ] || [ "$STATUS" = "30" ]; then echo "Failed!"; exit 1; fi; sleep 15; done'
TEST_CMD = 'curl -s -X POST -H "Authorization: Token ' + DBT_API_TOKEN + '" -H "Content-Type: application/json" -d \'{"cause": "Data quality check by Airflow"}\' "https://cloud.getdbt.com/api/v2/accounts/' + DBT_ACCOUNT_ID + '/jobs/' + DBT_TEST_JOB_ID + '/run/"'

def notify_success(context):
    print(f"Pipeline succeeded! DAG: {context['dag'].dag_id}")
    print(f"Run ID: {context['run_id']}")

def notify_failure(context):
    print(f"Pipeline FAILED! DAG: {context['dag'].dag_id}")
    print(f"Failed task: {context['task_instance'].task_id}")

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi ELT pipeline using dbt Cloud',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'dbt'],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure
) as dag:

    wait_for_dbt_api = BashSensor(
        task_id='wait_for_dbt_api',
        bash_command=SENSOR_CMD,
        poke_interval=30,
        timeout=300
    )

    trigger_dbt_build = BashOperator(
        task_id='trigger_dbt_build',
        bash_command=BUILD_CMD
    )

    check_data_quality = BashOperator(
        task_id='check_data_quality',
        bash_command=TEST_CMD
    )

    wait_for_dbt_api >> trigger_dbt_build >> check_data_quality
