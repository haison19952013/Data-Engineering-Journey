import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from pandas import json_normalize


def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['users'][0]
    processed_user = json_normalize({
        'id': user['id'],
        'firstname': user['firstName'],
        'lastname': user['lastName'],
        'country': user['address']['country'],
        'username': user['username'],
        'password': user['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=False, header=False)


with DAG('user_processing', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='users?limit=10',
        timeout=60,
        poke_interval=20
    )

    extract_user = HttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='users?limit=10',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user
    )

    is_api_available >> extract_user >> process_user
