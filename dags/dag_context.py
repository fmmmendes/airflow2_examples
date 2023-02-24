#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
import logging
import shutil
import time
from pprint import pprint
from datetime import (datetime, timedelta, date)

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator

log = logging.getLogger(__name__)

def print_context_old(**kwargs):
    
    pprint(kwargs)
    
    ds = kwargs['ds']
    print(f'This is dag run date {ds}')
    
    ti = kwargs['ti']
    sd = ti.start_date

    print(f'This is start_date {sd}')
    print(type(sd)) # original task instance start date as datetime data type
    
    #from python operator arguments
    task_inst_start_date = kwargs['task_inst_start_date']
    print("task_inst_start_date: " + str(task_inst_start_date))
    print(type(task_inst_start_date)) # this one is the same as "sd" but it comes as a string data type
    
    
default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}
    

with DAG(
    dag_id='dag_context',
    schedule_interval="0 5 * * *",
    start_date=pendulum.datetime(2023, 2, 10, tz="UTC"),
    catchup=False,
    tags=['aiflow2_examples'],
) as dag:
    
    ## Old Way to create tasks
    
    t_print_context_old = PythonOperator(
        task_id="print_context_old",
        python_callable=print_context_old,
        op_kwargs=
        {   
            "task_inst_start_date":"{{ti.start_date}}"

        },
        retries=2,
        dag=dag
    )
    
    ## New Way to create tasks

    @task(task_id="print_context_new",
          provide_context=True)
    def print_context_new(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        ti = kwargs['ti']
        
        sd = ti.start_date

        print(f'This is start_date {sd}')
        print(type(sd))

    t_print_context_new = print_context_new()
    
    t_print_context_old >> t_print_context_new
    