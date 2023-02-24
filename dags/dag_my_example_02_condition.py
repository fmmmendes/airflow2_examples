import json
import logging
import os
import pytz
import requests
import pendulum
import time
from datetime import (datetime, timedelta, date)
from functools import partial

from pandas import DataFrame
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_format

#set_flow = "B"
set_flow = "C"

def print_step(step_name,fail):
    print('This is step: ' + step_name)
    
    if fail:
        raise
    

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with models.DAG(
    "dag_my_example_02_condition",
    schedule_interval=None,
    start_date=pendulum.datetime(2022,11, 20, tz="UTC"),
    catchup=False,
    default_args = default_args,
    tags=['aiflow2_examples']
) as dag:

    
    print_data_a = PythonOperator(
        task_id="print_data_a",
        python_callable=print_step,
        op_kwargs=
        {   
            "date" : "{{ ds }}",
            "step_name":"A",
            "fail":False
        },
        dag=dag
    )
    
    if set_flow == "B":
        print_data_b = PythonOperator(
            task_id="print_data_b",
            python_callable=print_step,
            op_kwargs=
            {   
                "date" : "{{ ds }}",
                "step_name":"B",
                "fail":False
            },
            dag=dag
        )
    
    if set_flow == "B" or set_flow == "C":
        print_data_c = PythonOperator(
            task_id="print_data_c",
            python_callable=print_step,
            op_kwargs=
            {   
                "date" : "{{ ds }}",
                "step_name":"C",
                "fail":False
            },
            dag=dag
        )

if set_flow == "B":
    print_data_a >> [print_data_b,
                     print_data_c]
    
elif set_flow == "C":
    print_data_a >> print_data_c