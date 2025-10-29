
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "17",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/california_schools/satscores'},'product_nr':'17_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'AvgScrWrite': {'min': 500}}},'product_nr':'17_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/california_schools/schools'},'product_nr':'17_2'}
    )

    t3 = PythonOperator(
        task_id="filter_product_2",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'CharterNum': {'not_null': True}}},'product_nr':'17_2'}
    )

    t4 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'CDSCode', 'columns_right': 'cds', 'type': 'equals', 'values': ['None']},'left_product':'17_1','right_product':'17_2' }
    )

    t5 = PythonOperator(
        task_id="retrieve_product_4",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/california_schools/frpm'},'product_nr':'17_4'}
    )

    t6 = PythonOperator(
        task_id="combine_product_3_product_4",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'CDSCode', 'columns_right': 'CDSCode', 'type': 'equals', 'values': ['None']},'left_product':'17_3','right_product':'17_4' }
    )

    t7 = PythonOperator(
        task_id="mean_product_5",
        python_callable = tasks.mean_as_request_task,
        op_kwargs = {'filter_dict':{'group_by': 'CDSCode', 'columns': 'AvgScrWrite'},'product_nr':'17_5'}
    )

    t8 = PythonOperator(
        task_id="sortby_product_5",
        python_callable = tasks.sortby_as_request_task,
        op_kwargs = {'filter_dict':{'columns': 'AvgScrWrite', 'order': 'descending'},'product_nr':'17_5'}
    )

    t9 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"17"}
    )

    t0 >> t1
    t2 >> t3
    t1 >> t4
    t3 >> t4
    t4 >> t6
    t5 >> t6
    t6 >> t7
    t7 >> t8
    t8 >> t9