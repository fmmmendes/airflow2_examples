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

## Some References about Templates
# https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
# https://docs.astronomer.io/learn/templating
# https://stackoverflow.com/questions/44513308/macros-in-the-airflow-python-operator
# https://diogoalexandrefranco.github.io/about-airflow-date-macros-ds-and-execution-date/

# Notes

# The "ds" var is the same as "data_interval_start" but in a different format

# Formats
ds_template = "{{ ds }}" # 2023-02-28
ds_format_template = "{{macros.ds_format(ds,'%Y-%m-%d' ,'%Y%m%d')}}" # 20230228
data_interval_start_template = "{{ data_interval_start }}" # "2023-02-28T06:00:00+00:00"
data_interval_start_ds_template = "{{ data_interval_start | ds }}" # "2023-02-28"
data_interval_start_ds_nodash_template = "{{ data_interval_start | ds_nodash }}" # "20230228"

# Date Operations
ds_add1_template = "{{ macros.ds_add( ds, 1) }}" # "2023-03-01"
ds_rem1_template = "{{ macros.ds_add( ds, -1) }}" # "2023-02-27"

data_interval_start_add1_ds_template = "{{ macros.ds_add( data_interval_start | ds ,1) }}" # "2023-03-01"
data_interval_start_rem1_ds_template = "{{ macros.ds_add( data_interval_start | ds ,-1) }}" #"2023-02-27"
data_interval_end_rem1_ds_template = "{{ macros.ds_add( data_interval_end | ds ,-1) }}" #"2023-02-28"

# Operation and Format
ds_add1_formart_ymd_template = "{{ macros.ds_format(macros.ds_add( ds, 1), '%Y-%m-%d' ,'%Y%m%d') }}"
ds_rem1_formart_ymd_template = "{{ macros.ds_format(macros.ds_add( ds, -1), '%Y-%m-%d' ,'%Y%m%d') }}"

data_interval_start_add1_ds_nodash_template = "{{ macros.ds_format(macros.ds_add( data_interval_start | ds, 1), '%Y-%m-%d' ,'%Y%m%d') }}" #"20230301"
data_interval_end_rem1_ds_nodash_template = "{{ macros.ds_format(macros.ds_add( data_interval_end | ds, -1), '%Y-%m-%d' ,'%Y%m%d') }}" #"20230228"

## Combinations with Templates
my_var_txt_and_template = 'This is DAG run date: ' +  ds_template

def print_dates(**kwargs):
    
    print('print kwargs')
    pprint(kwargs)
    
    # print('print vars')
    # #from python operator arguments
    # task_inst_start_date = kwargs['task_inst_start_date']
    # print("task_inst_start_date: " + str(task_inst_start_date))
    # print(type(task_inst_start_date)) # this one is the same as "sd" but it comes as a string data type
    
    # print(kwargs['my_var_txt_and_template'])
    
    # ti = kwargs['ti']
    
    # print(ti['start_date'])
    # print(ti['end_date'])
    # print(ti['duration'])
    
    
default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}
    

with DAG(
    dag_id='dag_my_example_03_dates.py',
    schedule_interval = "0 6 * * TUE-SAT",
    start_date=pendulum.datetime(2023, 2, 16, tz="UTC"),
    end_date=pendulum.datetime(2023, 3, 2, tz="UTC"),
    catchup=True,
    tags=['aiflow2_examples'],
) as dag:
    
    ## Old Way to create tasks
    
    t_print_dates = PythonOperator(
        task_id="print_dates",
        python_callable=print_dates,
        op_kwargs=
        {   
            "ds_template":ds_template,
            "ds_format_template":ds_format_template,
            "data_interval_start_template":data_interval_start_template,
            "data_interval_start_ds_template":data_interval_start_ds_template,
            "data_interval_start_ds_nodash_template":data_interval_start_ds_nodash_template,
            "ds_add1_template":ds_add1_template,
            "ds_rem1_template":ds_rem1_template,
            "data_interval_start_add1_ds_template":data_interval_start_add1_ds_template,
            "data_interval_start_rem1_ds_template":data_interval_start_rem1_ds_template,
            "data_interval_end_rem1_ds_template":data_interval_end_rem1_ds_template,
            "data_interval_start_add1_ds_nodash_template":data_interval_start_add1_ds_nodash_template,
            "ds_add1_formart_ymd_template":ds_add1_formart_ymd_template,
            "ds_rem1_formart_ymd_template":ds_rem1_formart_ymd_template,
            "data_interval_start_add1_ds_nodash_template":data_interval_start_add1_ds_nodash_template,
            "data_interval_end_rem1_ds_nodash_template":data_interval_end_rem1_ds_nodash_template,
            "task_inst_start_date":"{{ti.start_date}}",
            "task_inst_end_date":"{{ti.end_date}}",
            "task_inst_duration":"{{ti.duration}}",
            "end_date":"{{ts.end_date}}",
            "my_var_txt_and_template":my_var_txt_and_template

        },
        retries=2,
        provide_context=True, #
        dag=dag
    )
    
    t_print_dates
    