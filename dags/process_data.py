from datetime import datetime
from textwrap import dedent

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from airflow.models import Variable

install_imdb = dedent(
    """
    pip install IMDbPY
    """
)

with DAG(
        dag_id="process_data_v3",
        schedule_interval=None,
        start_date=datetime(year=2021, day=2, month=7, hour=23, minute=13),
        catchup=False,
        tags=['FreeUni']
) as dag:
    airflow_host = '/airflow'
    jobs_path = f'{airflow_host}/jobs'
    cast_members_app = 'cast_members.py'
    meta_app = 'meta.py'
    keywords_app = 'keywords.py'
    ratings_app = 'ratings.py'

    imdb_installer = BashOperator(task_id="install_imdb_module", bash_command=install_imdb, depends_on_past=False)

    process_cast_pyspark_submit = SparkSubmitOperator(application=f'{jobs_path}/{cast_members_app}',
                                                      task_id="cast_members_submit")

    meta_pyspark_submit = SparkSubmitOperator(application=f'{jobs_path}/{meta_app}',
                                              task_id="meta_submit")

    keywords_pyspark_submit = SparkSubmitOperator(application=f'{jobs_path}/{keywords_app}',
                                                  task_id="keywords_submit")

    ratings_pyspark_submit = SparkSubmitOperator(application=f'{jobs_path}/{ratings_app}',
                                                 task_id="ratings_submit")

    with TaskGroup("save_images_group") as save_images_group:
        num_processes = int(Variable.get("num_processes"))

        for i in range(num_processes):
            SparkSubmitOperator(application=f'{jobs_path}/save_image.py', task_id=f'save_image_{i}',
                                application_args=[str(i), str(num_processes)])

    imdb_installer >> [process_cast_pyspark_submit, meta_pyspark_submit,
                       keywords_pyspark_submit] >> ratings_pyspark_submit >> save_images_group
