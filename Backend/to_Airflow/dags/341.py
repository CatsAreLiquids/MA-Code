
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "341",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/card_games/cards'},'product_nr':'341_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'borderColor': 'borderless'}},'product_nr':'341_1'}
    )

    t2 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'cardKingdomId': {'not_null': True}, 'cardKingdomFoilId': {'null': True}}},'product_nr':'341_1'}
    )

    t3 = PythonOperator(
        task_id="combine_product_0_product_1",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'cardKingdomId', 'columns_right': 'cardKingdomId', 'type': 'equals', 'values': ['None']},'left_product':'341_0','right_product':'341_1' }
    )

    t4 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"341"}
    )

    t0 >> t1
    t1 >> t2
    t-1 >> t3
    t2 >> t3
    t3 >> t4