
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "93",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/debit_card_specializing/transactions_1k'},'product_nr':'93_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'gender': 'M'}},'product_nr':'93_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/california_schools/schools'},'product_nr':'93_2'}
    )

    t3 = PythonOperator(
        task_id="filter_product_2",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'A3': 'North Bohemia', 'A11': {'min': 8000}}},'product_nr':'93_2'}
    )

    t4 = PythonOperator(
        task_id="retrieve_product_3",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/client'},'product_nr':'93_3'}
    )

    t5 = PythonOperator(
        task_id="retrieve_product_4",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/district'},'product_nr':'93_4'}
    )

    t6 = PythonOperator(
        task_id="combine_product_3_product_4",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'district_id', 'columns_right': 'district_id', 'type': 'equals', 'values': ['None']},'left_product':'93_3','right_product':'93_4' }
    )

    t7 = PythonOperator(
        task_id="combine_product_4_product_5",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'client_id', 'columns_right': 'client_id', 'type': 'equals', 'values': ['None']},'left_product':'93_4','right_product':'93_5' }
    )

    t8 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"93"}
    )

    t0 >> t1
    t2 >> t3
    t4 >> t6
    t5 >> t6
    t4 >> t7
    t6 >> t7
    t7 >> t8