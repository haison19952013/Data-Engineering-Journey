import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _say_hello():
    print("Hello Airflow")
    time.sleep(5)


with DAG(
        dag_id="hello_airflow",
        schedule='*/1 * * * *',
        start_date=datetime(2025, 1, 1),
        catchup=False
) as dag:
    say_hello = PythonOperator(
        task_id='say_hello',
        python_callable=_say_hello
    )
