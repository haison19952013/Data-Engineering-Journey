from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

csv_ds = Dataset(uri='/tmp/processed_user.csv')


def _store_user():
    hook = PostgresHook(
        postgres_conn_id='postgres',
        database='airflow'
    )

    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )


with DAG('user_storing', schedule=[csv_ds], start_date=datetime(2025, 1, 1), catchup=False) as dag:
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

    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )

    create_table >> store_user
