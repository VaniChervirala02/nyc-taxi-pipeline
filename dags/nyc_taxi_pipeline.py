from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'vani',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

DBT_ACCOUNT_ID = "271288"
DBT_JOB_ID = "1082994"

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi ELT pipeline using dbt Cloud',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'dbt']
) as dag:

    trigger_dbt_job = BashOperator(
        task_id='trigger_dbt_build',
        bash_command=f"""
            curl -s -X POST \
            -H "Authorization: Token ${{DBT_API_TOKEN}}" \
            -H "Content-Type: application/json" \
            -d '{{"cause": "Triggered by Airflow"}}' \
            "https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/" \
            | python3 -c "import sys,json; r=json.load(sys.stdin); print('Run ID:', r['data']['id']); exit(0 if r['status']['is_success'] else 1)"
        """
    )
