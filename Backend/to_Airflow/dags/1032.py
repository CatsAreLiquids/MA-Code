
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "1032",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/european_football_2/Match'},'product_nr':'1032_1'}
    )

    t1 = PythonOperator(
        task_id="count_product_1",
        python_callable = tasks.count_as_request_task,
        op_kwargs = {'filter_dict':{'group_by': 'league_id', 'column': 'id'},'product_nr':'1032_1'}
    )

    t2 = PythonOperator(
        task_id="sortby_product_1",
        python_callable = tasks.sortby_as_request_task,
        op_kwargs = {'filter_dict':{'columns': 'count', 'order': 'descending'},'product_nr':'1032_1'}
    )

    t3 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/european_football_2/League'},'product_nr':'1032_2'}
    )

    t4 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'league_id', 'columns_right': 'id', 'type': 'equals', 'values': ['None']},'left_product':'1032_1','right_product':'1032_2' }
    )

    t5 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"1032"}
    )

    t0 >> t1
    t1 >> t2
    t2 >> t4
    t3 >> t4
    t4 >> t5