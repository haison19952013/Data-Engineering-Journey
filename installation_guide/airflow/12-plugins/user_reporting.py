from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from hooks.user.user_report_hook import UserReportHook

csv_ds = Dataset(uri='/tmp/processed_user.csv')


def _report_user():
    hook = PostgresHook(
        postgres_conn_id='postgres',
        database='airflow'
    )

    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )


@dag(dag_id='user_reporting', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False)
def user_reporting():
    @task(task_id='report_user')
    def report_user():
        hook = UserReportHook(
            conn_id='postgres',
            dbname='airflow'
        )

        hook.report()

    report_user()


user_reporting()
