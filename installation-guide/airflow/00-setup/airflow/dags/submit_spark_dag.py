from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(dag_id='hello_spark', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False)
def hello_spark():
    submit_hello_spark = SparkSubmitOperator(
        task_id='submit_hello_spark',
        application='dags/include/hello_spark.py',
        conn_id='spark_conn',
        verbose=True,
        name='hello_spark',
    )


hello_spark()
