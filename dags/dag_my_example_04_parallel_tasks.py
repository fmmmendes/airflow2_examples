from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.branch_operator import BranchOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.utils.edgemodifier import Label

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 3)
}

with DAG('dag_my_example_04_parallel_tasks', 
         default_args=default_args, 
         schedule_interval=None,
         tags=['aiflow2_examples'],
         ) as dag:

    # First task
    start_task = DummyOperator(task_id='start_task')
    
    # Task group for parallel tasks
    with TaskGroup('parallel_tasks') as parallel_group:
        for i in range(3):  # N=3 in this example
            task_id = f'parallel_task_{i}'
            task = DummyOperator(task_id=task_id)
    
    # Joining task
    join_task = DummyOperator(task_id='join_task')
    
    start_task >> parallel_group >> join_task
    