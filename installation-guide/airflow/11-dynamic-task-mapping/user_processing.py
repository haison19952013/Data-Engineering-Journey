from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import json_normalize

PAGE_SIZE = 50


@dag(dag_id='user_processing', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False)
def user_processing():
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='users?limit=10',
        timeout=60,
        poke_interval=20
    )

    @task(task_id='get_pages')
    def get_pages():
        conn = BaseHook.get_connection('user_api')
        res = requests.get(conn.host + f'users?limit={PAGE_SIZE}')
        res = res.json()
        print(res)
        total = res['total']
        total_pages = int(total / PAGE_SIZE)
        if total % PAGE_SIZE != 0:
            total_pages += 1
        return [i for i in range(1, total_pages + 1)]

    @task(task_id='extract_user')
    def extract_user(page_no=1):
        conn = BaseHook.get_connection('user_api')
        res = requests.get(conn.host + f'users?limit={PAGE_SIZE}&skip={(page_no - 1) * PAGE_SIZE}')
        res = res.json()
        print(res)
        return res['users']

    @task(task_id='process_user')
    def process_user(page_no_users):
        page_no, users = page_no_users
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

    @task(task_id='store_user')
    def store_user(page_no=1):
        hook = PostgresHook(
            postgres_conn_id='postgres',
            database='airflow'
        )

        hook.copy_expert(
            sql="COPY users FROM stdin WITH DELIMITER as ','",
            filename=f'/tmp/processed_user_{page_no}.csv'
        )

    _pages = get_pages()

    _extract_users = extract_user.expand(page_no=_pages)

    _page_no_users = _pages.zip(_extract_users)

    _process_users = process_user.expand(page_no_users=_page_no_users)

    _store_users = store_user.expand(page_no=_pages)

    is_api_available >> _pages >> _extract_users >> _process_users >> create_table >> _store_users


user_processing()
