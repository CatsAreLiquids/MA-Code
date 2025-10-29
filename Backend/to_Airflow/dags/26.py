
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "26",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/codebase_community/posts'},'product_nr':'26_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'location': 'Monterey', 'FRPM Count (Ages 5-17)': {'min': 800}}},'product_nr':'26_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/california_schools/frpm'},'product_nr':'26_2'}
    )

    t3 = PythonOperator(
        task_id="filter_product_2",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'County Name': 'Monterey', 'FRPM Count (Ages 5-17)': {'min': 800}}},'product_nr':'26_2'}
    )

    t4 = PythonOperator(
        task_id="retrieve_product_3",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/california_schools/schools'},'product_nr':'26_3'}
    )

    t5 = PythonOperator(
        task_id="filter_product_3",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'County': 'Monterey', 'EILName': 'High School'}},'product_nr':'26_3'}
    )

    t6 = PythonOperator(
        task_id="combine_product_2_product_3",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'CDSCode', 'columns_right': 'CDSCode', 'type': 'equals', 'values': ['None']},'left_product':'26_2','right_product':'26_3' }
    )

    t7 = PythonOperator(
        task_id="returnResult_product_4",
        python_callable = tasks.returnResult_as_request_task,
        op_kwargs = {'filter_dict':{'columns': ['School Name', 'Street', 'City', 'State', 'Zip']},'product_nr':'26_4'}
    )

    t8 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"26"}
    )

    t0 >> t1
    t2 >> t3
    t4 >> t5
    t3 >> t6
    t5 >> t6
    t6 >> t7
    t7 >> t8