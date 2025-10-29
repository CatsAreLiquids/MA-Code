
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "112",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/debit_card_specializing/transactions_1k'},'product_nr':'112_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'gender': 'F', 'birth_date': '1976-01-29'}},'product_nr':'112_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/debit_card_specializing/products'},'product_nr':'112_2'}
    )

    t3 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'ProductID', 'columns_right': 'ProductID', 'type': 'equals', 'values': ['None']},'left_product':'112_1','right_product':'112_2' }
    )

    t4 = PythonOperator(
        task_id="retrieve_product_4",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/student_club/Member'},'product_nr':'112_4'}
    )

    t5 = PythonOperator(
        task_id="combine_product_3_product_4",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'Id', 'columns_right': 'member_id', 'type': 'equals', 'values': ['None']},'left_product':'112_3','right_product':'112_4' }
    )

    t6 = PythonOperator(
        task_id="retrieve_product_6",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/california_schools/schools'},'product_nr':'112_6'}
    )

    t7 = PythonOperator(
        task_id="combine_product_5_product_6",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'district_id', 'columns_right': 'district_id', 'type': 'equals', 'values': ['None']},'left_product':'112_5','right_product':'112_6' }
    )

    t8 = PythonOperator(
        task_id="retrieve_product_8",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/client'},'product_nr':'112_8'}
    )

    t9 = PythonOperator(
        task_id="filter_product_8",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'gender': 'F', 'birth_date': '1976-01-29'}},'product_nr':'112_8'}
    )

    t10 = PythonOperator(
        task_id="retrieve_product_9",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/disp'},'product_nr':'112_9'}
    )

    t11 = PythonOperator(
        task_id="combine_product_8_product_9",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'client_id', 'columns_right': 'client_id', 'type': 'equals', 'values': ['None']},'left_product':'112_8','right_product':'112_9' }
    )

    t12 = PythonOperator(
        task_id="retrieve_product_11",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/account'},'product_nr':'112_11'}
    )

    t13 = PythonOperator(
        task_id="combine_product_10_product_11",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'account_id', 'columns_right': 'account_id', 'type': 'equals', 'values': ['None']},'left_product':'112_10','right_product':'112_11' }
    )

    t14 = PythonOperator(
        task_id="retrieve_product_13",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/financial/district'},'product_nr':'112_13'}
    )

    t15 = PythonOperator(
        task_id="combine_product_12_product_13",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'district_id', 'columns_right': 'district_id', 'type': 'equals', 'values': ['None']},'left_product':'112_12','right_product':'112_13' }
    )

    t16 = PythonOperator(
        task_id="returnResult_product_14",
        python_callable = tasks.returnResult_as_request_task,
        op_kwargs = {'filter_dict':{},'product_nr':'112_14'}
    )

    t17 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"112"}
    )

    t0 >> t1
    t1 >> t3
    t2 >> t3
    t3 >> t5
    t4 >> t5
    t5 >> t7
    t6 >> t7
    t8 >> t9
    t9 >> t11
    t10 >> t11
    t11 >> t13
    t12 >> t13
    t13 >> t15
    t14 >> t15
    t15 >> t16
    t16 >> t17