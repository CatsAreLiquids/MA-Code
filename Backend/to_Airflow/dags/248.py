
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

import tasks

with DAG(
    "248",
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
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/toxicology/bond'},'product_nr':'248_1'}
    )

    t1 = PythonOperator(
        task_id="filter_product_1",
        python_callable = tasks.filter_as_request_task,
        op_kwargs = {'filter_dict':{'conditions': {'molecule_id': 'TR041', 'bond_type': '#'}},'product_nr':'248_1'}
    )

    t2 = PythonOperator(
        task_id="retrieve_product_2",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/toxicology/atom'},'product_nr':'248_2'}
    )

    t3 = PythonOperator(
        task_id="combine_product_1_product_2",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'molecule_id', 'columns_right': 'molecule_id', 'type': 'equals', 'values': ['None']},'left_product':'248_1','right_product':'248_2' }
    )

    t4 = PythonOperator(
        task_id="retrieve_product_4",
        python_callable = tasks.retrieve_as_request_task,
        op_kwargs = {'filter_dict':{'product': 'http://127.0.0.1:5000/products/toxicology/connected'},'product_nr':'248_4'}
    )

    t5 = PythonOperator(
        task_id="combine_product_3_product_4",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'atom_id', 'columns_right': 'atom_id', 'type': 'equals', 'values': ['None']},'left_product':'248_3','right_product':'248_4' }
    )

    t6 = PythonOperator(
        task_id="combine_product_4_product_5",
        python_callable = tasks.combination_as_request_task,
        op_kwargs = {'filter_dict':{'columns_left': 'bond_id', 'columns_right': 'bond_id', 'type': 'equals', 'values': ['None']},'left_product':'248_4','right_product':'248_5' }
    )

    t7 = PythonOperator(
        task_id="clean_up",
        python_callable = tasks.cleanup,
        op_kwargs = {"dag_id":"248"}
    )

    t0 >> t1
    t1 >> t3
    t2 >> t3
    t3 >> t5
    t4 >> t5
    t3 >> t6
    t5 >> t6
    t6 >> t7