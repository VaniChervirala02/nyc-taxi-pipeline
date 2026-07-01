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

SENSOR_CMD = (
    'curl -s -o /dev/null -w "%{http_code}" '
    '-H "Authorization: Token ' + DBT_API_TOKEN + '" '
    '"https://cloud.getdbt.com/api/v2/accounts/' + DBT_ACCOUNT_ID + '/" '
    '| grep -q "200"'
)

TRIGGER_CMD = (
    'curl -s -X POST '
    '-H "Authorization: Token ' + DBT_API_TOKEN
