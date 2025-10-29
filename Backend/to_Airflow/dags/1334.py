
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "1334",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/student_club/Member'},'product_nr':'1334_1'}
    )

    t1 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/student_club/Zip_Code'},'product_nr':'1334_2'}
    )

    t2 = PythonOperator(
        task_id="filter_product_2",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'state': 'Illinois'}},'product_nr':'1334_2'}
    )

    t3 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'zip', 'columns_right': 'zip_code', 'type': 'equals', 'values': ['None']},'left_product':'1334_1','right_product':'1334_2' }
    )

    t4 = PythonOperator(
        task_id="combine_product_2_product_3",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'first_name', 'columns_right': 'None', 'type': 'select', 'values': ['None']},'left_product':'1334_2','right_product':'1334_3' }
    )

    t5 = PythonOperator(
        task_id="combine_product_3_product_4",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'last_name', 'columns_right': 'None', 'type': 'select', 'values': ['None']},'left_product':'1334_3','right_product':'1334_4' }
    )

    t6 = PythonOperator(
        task_id="combine_product_4_product_5",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'member_id', 'columns_right': 'link_to_member', 'type': 'equals', 'values': ['None']},'left_product':'1334_4','right_product':'1334_5' }
    )

    t7 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"1334"}
    )

    t1 >> t2
    t0 >> t3
    t2 >> t3
    t0 >> t4
    t3 >> t4
    t0 >> t5
    t4 >> t5
    t0 >> t6
    t5 >> t6
    t6 >> t7