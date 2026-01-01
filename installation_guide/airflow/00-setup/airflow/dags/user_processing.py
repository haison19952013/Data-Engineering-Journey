from datetime import datetime

import requests
from airflow import DAG
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from pandas import json_normalize

csv_ds = Dataset(uri='/tmp/processed_user.csv')


def _extract_user(ti: TaskInstance):
    conn = BaseHook.get_connection('user_api')
    res = requests.get(conn.host + 'users?limit=10')
    res = res.json()
    print(res)
    users = res['users']
    ti.xcom_push(key='users', value=users)


def _process_user(ti):
    users = ti.xcom_pull(task_ids="extract_user", key='users')
    processed_users = [{
        'id': user['id'],
        'firstname': user['firstName'],
        'lastname': user['lastName'],
        'country': user['address']['country'],
        'username': user['username'],
        'password': user['password'],
        'email': user['email'],
        'age': user['age'],
        'gender': user['gender']
    } for user in users]

    processed_users = json_normalize(processed_users)

    processed_users.to_csv(csv_ds.uri, index=False, header=False)


with DAG('user_processing', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='users?limit=10',
        timeout=60,
        poke_interval=20
    )

    extract_user = PythonOperator(
        task_id='extract_user',
        python_callable=_extract_user
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user,
        outlets=[csv_ds]
    )

    is_api_available >> extract_user >> process_user
