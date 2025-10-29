
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "1251",
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

    t0 = PythonOperator(
        task_id="retrieve_product_1",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/thrombosis_prediction/Laboratory'},'product_nr':'1251_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'IGG': {'min': 2000}}},'product_nr':'1251_1'}
    )

    t2 = PythonOperator(
        task_id="count_product_1",
        python_callable = tasks.count_as_request_task,
        op_kwargs = {'filter_dict':{'distinct': True, 'column': 'ID'},'product_nr':'1251_1'}
    )

    t3 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"1251"}
    )

    t0 >> t1
    t1 >> t2
    t2 >> t3