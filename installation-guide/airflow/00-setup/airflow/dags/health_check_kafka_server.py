import socket
from contextlib import closing
from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator, BranchPythonOperator


def _health_check(ti: TaskInstance):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex(('113.160.15.232', 9095)) == 0:
            print("Kafka server is up")
            ti.xcom_push(key='health_check_status', value=1)
        else:
            print("Kafka server is down")
            ti.xcom_push(key='health_check_status', value=0)


def _branch(ti: TaskInstance):
    health_check_status = ti.xcom_pull(task_ids='health_check', key='health_check_status')
    if health_check_status == 1:
        return 'server_is_up'
    return 'server_is_down'


def _server_is_up():
    print("Kafka server is up")


def _server_is_down():
    print("Kafka server is down")
    # TODO: alert


with DAG(
        dag_id="health_check_kafka_server",
        schedule="*/2 * * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False
) as dag:
    health_check = PythonOperator(
        task_id='health_check',
        python_callable=_health_check
    )

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=_branch
    )

    server_is_up = PythonOperator(
        task_id='server_is_up',
        python_callable=_server_is_up
    )

    server_is_down = PythonOperator(
        task_id='server_is_down',
        python_callable=_server_is_down
    )

    health_check >> branch >> [server_is_up, server_is_down]
