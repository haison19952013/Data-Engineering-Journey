import json
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

with DAG('user_processing', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    extract_user = HttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='users?limit=10',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
