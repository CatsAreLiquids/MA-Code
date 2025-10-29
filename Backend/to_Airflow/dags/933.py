
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "933",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/formula_1/races'},'product_nr':'933_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'name': 'Chinese Grand Prix', 'year': 2008}},'product_nr':'933_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/formula_1/results'},'product_nr':'933_2'}
    )

    t3 = PythonOperator(
        task_id="retrieve_product_3",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/formula_1/drivers'},'product_nr':'933_3'}
    )

    t4 = PythonOperator(
        task_id="filter_product_3",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'forename': 'Lewis', 'surname': 'Hamilton'}},'product_nr':'933_3'}
    )

    t5 = PythonOperator(
        task_id="combine_product_2_product_3",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'driverId', 'columns_right': 'driverId', 'type': 'equals', 'values': ['None']},'left_product':'933_2','right_product':'933_3' }
    )

    t6 = PythonOperator(
        task_id="combine_product_3_product_4",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'raceId', 'columns_right': 'raceId', 'type': 'equals', 'values': ['None']},'left_product':'933_3','right_product':'933_4' }
    )

    t7 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"933"}
    )

    t0 >> t1
    t3 >> t4
    t2 >> t5
    t4 >> t5
    t2 >> t6
    t5 >> t6
    t6 >> t7