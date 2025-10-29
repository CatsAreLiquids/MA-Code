
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "1147",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/european_football_2/Player_Attributes'},'product_nr':'1147_1'}
    )

    t1 = PythonOperator(
        task_id="max_product_1",
        python_callable = tasks.max_as_request_task,
        op_kwargs = {'filter_dict':{'columns': 'overall_rating'},'product_nr':'1147_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/european_football_2/Player'},'product_nr':'1147_2'}
    )

    t3 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'player_api_id', 'columns_right': 'player_api_id', 'type': 'equals', 'values': ['None']},'left_product':'1147_1','right_product':'1147_2' }
    )

    t4 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"1147"}
    )

    t0 >> t1
    t1 >> t3
    t2 >> t3
    t3 >> t4