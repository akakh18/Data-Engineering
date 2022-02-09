from datetime import datetime, timedelta
from textwrap import dedent

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 2
}

with DAG(
        dag_id="process_data_v3",
        default_args=default_args,
        schedule_interval=timedelta(seconds=30),
        start_date=datetime(year=2022, day=2, month=7, hour=23, minute=13),
        catchup=False,
        tags=['FreeUni'],
        max_active_runs=1,
) as dag:
    airflow_host = '/airflow'
    jobs_path = f'{airflow_host}/jobs'
    cast_members_app = 'cast_members.py'
    meta_app = 'meta.py'

    process_cast_pyspark_submit = SparkSubmitOperator(application=f'{jobs_path}/{cast_members_app}',
                                                      task_id="cast_members_submit")

    meta_pyspark_submit = SparkSubmitOperator(application=f'{jobs_path}/{meta_app}',
                                              task_id="meta_submit")

    process_cast_pyspark_submit >> meta_pyspark_submit
