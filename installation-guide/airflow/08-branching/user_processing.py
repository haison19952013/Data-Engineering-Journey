import random
from datetime import datetime

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import json_normalize


def _extract_user(ti: TaskInstance):
    conn = BaseHook.get_connection('user_api')
    res = requests.get(conn.host + 'users?limit=10')
    res = res.json()
    print(res)
    users = res['users']
    user = users[random.randint(0, len(users) - 1)]
    print(f"user: {user}")
    ti.xcom_push(key='user', value=user)


def _is_valid_user(ti: TaskInstance):
    user = ti.xcom_pull(task_ids='extract_user', key='user')
    if user['age'] > 30:
        return 'skip_user'
    return 'process_user'


def _skip_user():
    print("skipped user")


def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user", key='user')
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


def _store_user():
    hook = PostgresHook(
        postgres_conn_id='postgres',
        database='airflow'
    )

    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )


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

    is_valid_user = BranchPythonOperator(
        task_id='is_valid_user',
        python_callable=_is_valid_user
    )

    skip_user = EmptyOperator(
        task_id='skip_user'
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user
    )

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres',
        database='airflow',
        sql='''
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT NOT NULL,
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL
        );
        '''
    )

    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )

    is_api_available >> extract_user >> is_valid_user >> [skip_user, process_user]
    process_user >> create_table >> store_user
