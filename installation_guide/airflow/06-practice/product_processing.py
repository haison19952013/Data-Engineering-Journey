import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import json_normalize


def _process_product(ti):
    user = ti.xcom_pull(task_ids="extract_product")
    user = user['products'][0]
    processed_user = json_normalize({
        'id': user['id'],
        'title': user['title'],
        'description': user['description'],
        'category': user['category'],
        'price': user['price'],
        'discountPercentage': user['discountPercentage'],
        'rating': user['rating']
    })
    processed_user.to_csv('/tmp/processed_product.csv', index=False, header=False)


def _store_product():
    hook = PostgresHook(
        postgres_conn_id='postgres',
        database='airflow'
    )

    hook.copy_expert(
        sql="COPY products FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_product.csv'
    )


with DAG('product_processing', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='products?limit=10',
        timeout=60,
        poke_interval=20
    )

    extract_product = HttpOperator(
        task_id='extract_product',
        http_conn_id='user_api',
        endpoint='products?limit=10',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_product = PythonOperator(
        task_id='process_product',
        python_callable=_process_product
    )

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres',
        database='airflow',
        sql='''
        CREATE TABLE IF NOT EXISTS products (
            id BIGINT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            category TEXT NOT NULL,
            price NUMERIC NOT NULL,
            discountPercentage NUMERIC NOT NULL,
            rating NUMERIC NOT NULL
        );
        '''
    )

    store_product = PythonOperator(
        task_id='store_product',
        python_callable=_store_product
    )

    is_api_available >> extract_product >> process_product >> create_table >> store_product
