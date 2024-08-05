"""
DAG Name: data_001_replicate
Description: This DAG demonstrates how to document an Airflow DAG.
It includes a start task and example documentation.

Author: Dinda Paramitha
Last Updated: 2024-08-05
"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
import kumparde.data_001_replicate.module.replicate as data_001

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


def do_replicate():
    data_001.replicate(source_db_type="mysql",
                       source_schema="kumparde",
                       source_table="articles",
                       target_db_type="redshift",
                       target_schema="kumparde",
                       target_table="articles")


with DAG(
        'data_001_replicate',
        default_args=default_args,
        description='A simple replication from mysql to redshift',
        schedule_interval='0 * * * *',
        start_date=datetime(2024, 8, 5),
        catchup=False,
        tags=['replicate'],
) as dag:
    task_replicate = PythonOperator(
        task_id="task_replicate",
        python_callable=do_replicate
    )
    # [END basic_task]

    # [START documentation]
    task_replicate.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    task_replicate
