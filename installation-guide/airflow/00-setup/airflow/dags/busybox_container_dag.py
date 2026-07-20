from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator


def _branch(ti: TaskInstance):
    health_check_status = ti.xcom_pull(task_ids='run_busybox_container', key='return_value')
    if health_check_status == '1':
        return 'server_is_up'
    return 'server_is_down'


def _server_is_up():
    print("Kafka server is up")


def _server_is_down():
    print("Kafka server is down")
    # TODO: alert


with DAG(
        dag_id="busybox_container_dag",
        schedule="*/2 * * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False
) as dag:
    run_busybox_container = DockerOperator(
        task_id='run_busybox_container',
        image='busybox:1.37.0',
        container_name='airflow-busybox',
        auto_remove='force',
        command=["sh", "-c", "nc -z -v -w 2 113.160.15.232 9094 && echo '1' || echo '0'"],
        docker_url='unix://var/run/docker.sock',
        network_mode="bridge"
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

    run_busybox_container >> branch >> [server_is_up, server_is_down]
