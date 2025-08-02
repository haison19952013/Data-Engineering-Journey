from datetime import datetime

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from pandas import json_normalize


def _extract_user(ti: TaskInstance, page_no=1):
    conn = BaseHook.get_connection('user_api')
    res = requests.get(conn.host + f'users?limit={PAGE_SIZE}&skip={(page_no - 1) * PAGE_SIZE}')
    res = res.json()
    print(res)
    users = res['users']
    ti.xcom_push(key='users', value=users)


def _process_user(ti, page_no=1):
    users = ti.xcom_pull(task_ids=f"user_processing_tg_{page_no}.extract_user", key='users')
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

    processed_users.to_csv(f'/tmp/processed_user_{page_no}.csv', index=False, header=False)


def _store_user(page_no=1):
    hook = PostgresHook(
        postgres_conn_id='postgres',
        database='airflow'
    )

    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename=f'/tmp/processed_user_{page_no}.csv'
    )


pages = [1, 2, 3]

PAGE_SIZE = 10

user_processing_task_groups = []
user_storing_task_groups = []

with DAG('user_processing', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='users?limit=10',
        timeout=60,
        poke_interval=20
    )

    for page in pages:
        with TaskGroup(group_id=f'user_processing_tg_{page}') as user_processing_tg:
            extract_user = PythonOperator(
                task_id='extract_user',
                python_callable=_extract_user,
                op_kwargs={'page_no': page}
            )

            process_user = PythonOperator(
                task_id='process_user',
                python_callable=_process_user,
                op_kwargs={'page_no': page}
            )

            extract_user >> process_user

            user_processing_task_groups.append(user_processing_tg)

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres',
        database='airflow',
        sql='''
        DROP TABLE IF EXISTS users;
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT NOT NULL,
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NOT NULL,
            gender TEXT NOT NULL
        );
        '''
    )

    for page in pages:
        with TaskGroup(group_id=f'user_storing_tg_{page}') as user_storing_tg:
            store_user = PythonOperator(
                task_id='store_user',
                python_callable=_store_user,
                op_kwargs={'page_no': page}
            )

            user_storing_task_groups.append(user_storing_tg)

    is_api_available >> user_processing_task_groups >> create_table >> user_storing_task_groups
