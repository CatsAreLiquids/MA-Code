
task_dict= {"retrieve":"tasks.retrieve_as_request_task","filter":"tasks.filter_as_request_task",
            "combination":"tasks.combination_as_request_task"}

imports = """
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "<<task_name>>",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["user"],
) as dag:
"""

task_template = """
    <<name>> = PythonOperator(
        task_id="<<task_id>>",
        python_callable = <<python_callable>>,
        op_kwargs = <<op_kwargs>>
    )
"""

dependencies ="""
    <<dependency>>"""

cleanup = """
    <<name>> = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"<<dag_id>>"}
    )
"""