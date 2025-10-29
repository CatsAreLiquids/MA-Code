
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "327",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/toxicology/molecule'},'product_nr':'327_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'label': '-'}},'product_nr':'327_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/toxicology/atom'},'product_nr':'327_2'}
    )

    t3 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'molecule_id', 'columns_right': 'molecule_id', 'type': 'equals', 'values': ['None']},'left_product':'327_1','right_product':'327_2' }
    )

    t4 = PythonOperator(
        task_id="count_product_3",
        python_callable = tasks.count_as_request_task,
        op_kwargs = {'filter_dict':{'columns': 'molecule_id', 'value': '>5', 'unique': False},'product_nr':'327_3'}
    )

    t5 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"327"}
    )

    t0 >> t1
    t1 >> t3
    t2 >> t3
    t3 >> t4
    t4 >> t5