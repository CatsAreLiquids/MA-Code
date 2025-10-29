
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "1381",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/student_club/Attendance'},'product_nr':'1381_1'}
    )

    t1 = PythonOperator(
        task_id="count_product_1",
        python_callable = tasks.count_as_request_task,
        op_kwargs = {'filter_dict':{'columns': 'link_to_event', 'value': '>7', 'group_by': 'link_to_member'},'product_nr':'1381_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/student_club/Member'},'product_nr':'1381_2'}
    )

    t3 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'link_to_member', 'columns_right': 'member_id', 'type': 'equals', 'values': ['None']},'left_product':'1381_1','right_product':'1381_2' }
    )

    t4 = PythonOperator(
        task_id="returnResult_product_3",
        python_callable = tasks.returnResult_as_request_task,
        op_kwargs = {'filter_dict':{'columns': ['first_name', 'last_name']},'product_nr':'1381_3'}
    )

    t5 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"1381"}
    )

    t0 >> t1
    t1 >> t3
    t2 >> t3
    t3 >> t4
    t4 >> t5