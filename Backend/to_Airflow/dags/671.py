
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "671",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/codebase_community/badges'},'product_nr':'671_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'Name': 'Autobiographer'}},'product_nr':'671_1'}
    )

    t2 = PythonOperator(
        task_id="min_product_1",
        python_callable = tasks.min_as_request_task,
        op_kwargs = {'filter_dict':{'columns': 'Date'},'product_nr':'671_1'}
    )

    t3 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/codebase_community/users'},'product_nr':'671_2'}
    )

    t4 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'UserId', 'columns_right': 'Id', 'type': 'equals', 'values': ['None']},'left_product':'671_1','right_product':'671_2' }
    )

    t5 = PythonOperator(
        task_id="returnResult_product_3",
        python_callable = tasks.returnResult_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'DisplayName'},'product_nr':'671_3'}
    )

    t6 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"671"}
    )

    t0 >> t1
    t1 >> t2
    t2 >> t4
    t3 >> t4
    t4 >> t5
    t5 >> t6