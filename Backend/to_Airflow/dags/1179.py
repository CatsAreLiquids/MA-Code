
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "1179",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/thrombosis_prediction/Examination'},'product_nr':'1179_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'Diagnosis': 'SLE', 'Examination Date': '1994-02-19'}},'product_nr':'1179_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/thrombosis_prediction/Patient'},'product_nr':'1179_2'}
    )

    t3 = PythonOperator(
        task_id="combination_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'ID', 'columns_right': 'ID', 'type': 'equals', 'values': ['None']},'product_nr':'1179_2'}
    )

    t4 = PythonOperator(
        task_id="filter_product_2",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'Examination Date': '1993-11-12'}},'product_nr':'1179_2'}
    )

    t5 = PythonOperator(
        task_id="returnResult_product_2",
        python_callable = tasks.returnResult_as_request_task,
        op_kwargs = {'filter_dict':{'columns': ['ID', 'Examination Date', 'aCL IgM']},'product_nr':'1179_2'}
    )

    t6 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"1179"}
    )

    t0 >> t1
    t2 >> t3
    t3 >> t4
    t4 >> t5
    t5 >> t6