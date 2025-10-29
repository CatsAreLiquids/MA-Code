
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "1472",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/debit_card_specializing/yearmonth'},'product_nr':'1472_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'Date': {'between': [201201, 201212]}}},'product_nr':'1472_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/debit_card_specializing/gasstations'},'product_nr':'1472_2'}
    )

    t3 = PythonOperator(
        task_id="filter_product_2",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'Country': 'LAM'}},'product_nr':'1472_2'}
    )

    t4 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'GasStationID', 'columns_right': 'GasStationID', 'type': 'equals', 'values': ['None']},'left_product':'1472_1','right_product':'1472_2' }
    )

    t5 = PythonOperator(
        task_id="retrieve_product_4",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/debit_card_specializing/transactions_1k'},'product_nr':'1472_4'}
    )

    t6 = PythonOperator(
        task_id="combine_product_3_product_4",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'CustomerID', 'columns_right': 'CustomerID', 'type': 'equals', 'values': ['None']},'left_product':'1472_3','right_product':'1472_4' }
    )

    t7 = PythonOperator(
        task_id="combine_product_4_product_5",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'CustomerID', 'columns_right': 'CustomerID', 'type': 'equals', 'values': ['None']},'left_product':'1472_4','right_product':'1472_5' }
    )

    t8 = PythonOperator(
        task_id="min_product_6",
        python_callable = tasks.min_as_request_task,
        op_kwargs = {'filter_dict':{'columns': 'Consumption', 'rows': 1},'product_nr':'1472_6'}
    )

    t9 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"1472"}
    )

    t0 >> t1
    t2 >> t3
    t1 >> t4
    t3 >> t4
    t4 >> t6
    t5 >> t6
    t4 >> t7
    t6 >> t7
    t7 >> t8
    t8 >> t9