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

def print_context(**kwargs):
    
    pprint(kwargs)
    
    ds = kwargs['ds']
    print(f'This is dag run date {ds}')
    
    ti = kwargs['ti']
    sd = ti.start_date
    print(f'This is task instance start date {sd}')
    
    dr = kwargs['dag_run']
    print(f'This is dag run {dr}')
    
    ext_trigger = kwargs['dag_run'].external_trigger
    print(f'This is external_trigger {ext_trigger}')
    
    #ext_tgg = dr.external_trigger
    #print(f'This is external_trigger {ext_tgg}')

    
    #from python operator arguments
    var = kwargs['var']
    print("var: " + str(var))
    
    return 'OK'
    
    
default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}
    

with DAG(
    dag_id='dag_my_example_06_external_trigger',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    #end_date=pendulum.datetime(2023, 1, 15, tz="UTC"),
    catchup=False,
    tags=['aiflow2_examples'],
) as dag:
    
    ## Old Way to create tasks
    
    t_print_context = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
        op_kwargs=
        {   
            "var":"{{dag_run.external_trigger}}",

        },
        retries=2,
        dag=dag
    )
    
    ## New Way to create tasks

    
    t_print_context
      