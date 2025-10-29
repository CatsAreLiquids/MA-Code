
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "92",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/california_schools/schools'},'product_nr':'92_1'}
    )

    t1 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/debit_card_specializing/transactions_1k'},'product_nr':'92_2'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_3",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/client'},'product_nr':'92_3'}
    )

    t3 = PythonOperator(
        task_id="retrieve_product_4",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/district'},'product_nr':'92_4'}
    )

    t4 = PythonOperator(
        task_id="combine_product_3_product_4",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'district_id', 'columns_right': 'district_id', 'type': 'equals', 'values': ['None']},'left_product':'92_3','right_product':'92_4' }
    )

    t5 = PythonOperator(
        task_id="filter_product_5",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'gender': 'F'}},'product_nr':'92_5'}
    )

    t6 = PythonOperator(
        task_id="filter_product_5",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'A11': {'min': 6000, 'max': 10000}}},'product_nr':'92_5'}
    )

    t7 = PythonOperator(
        task_id="count_product_5",
        python_callable = tasks.count_as_request_task,
        op_kwargs = {'filter_dict':{'columns': 'district_id', 'unique': True},'product_nr':'92_5'}
    )

    t8 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"92"}
    )

    t2 >> t4
    t3 >> t4
    t4 >> t5
    t5 >> t6
    t6 >> t7
    t7 >> t8