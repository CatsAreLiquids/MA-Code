
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "137",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/student_club/Member'},'product_nr':'137_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'district_id': 'Branch location 1'}},'product_nr':'137_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/debit_card_specializing/transactions_1k'},'product_nr':'137_2'}
    )

    t3 = PythonOperator(
        task_id="filter_product_2",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'status': ['C', 'D']}},'product_nr':'137_2'}
    )

    t4 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'member_id', 'columns_right': 'CustomerID', 'type': 'equals', 'values': ['None']},'left_product':'137_1','right_product':'137_2' }
    )

    t5 = PythonOperator(
        task_id="retrieve_product_4",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/district'},'product_nr':'137_4'}
    )

    t6 = PythonOperator(
        task_id="filter_product_4",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'district_id': 'Branch location 1'}},'product_nr':'137_4'}
    )

    t7 = PythonOperator(
        task_id="retrieve_product_5",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/account'},'product_nr':'137_5'}
    )

    t8 = PythonOperator(
        task_id="retrieve_product_6",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/loan'},'product_nr':'137_6'}
    )

    t9 = PythonOperator(
        task_id="combine_product_5_product_6",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'district_id', 'columns_right': 'district_id', 'type': 'equals', 'values': ['None']},'left_product':'137_5','right_product':'137_6' }
    )

    t10 = PythonOperator(
        task_id="combine_product_6_product_7",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'account_id', 'columns_right': 'account_id', 'type': 'equals', 'values': ['None']},'left_product':'137_6','right_product':'137_7' }
    )

    t11 = PythonOperator(
        task_id="count_product_8",
        python_callable = tasks.count_as_request_task,
        op_kwargs = {'filter_dict':{'columns': 'account_id', 'unique': True},'product_nr':'137_8'}
    )

    t12 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"137"}
    )

    t0 >> t1
    t2 >> t3
    t1 >> t4
    t3 >> t4
    t5 >> t6
    t7 >> t9
    t8 >> t9
    t7 >> t10
    t9 >> t10
    t10 >> t11
    t11 >> t12