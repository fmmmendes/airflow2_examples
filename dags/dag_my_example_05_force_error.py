from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.branch_operator import BranchOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.utils.edgemodifier import Label


def f_force_error():
    """This is a funtion to force an error
    
    https://docs.python.org/3/library/exceptions.html
    """
    
    raise Exception("This is a force error")
        

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 3)
}

with DAG('dag_my_example_05_force_error', 
         default_args=default_args, 
         schedule_interval=None,
         tags=['aiflow2_examples'],
         ) as dag:

    # First task
    t_force_error = PythonOperator(
        task_id = "force_error",
        python_callable=f_force_error
        
    )
    
    t_force_error
    