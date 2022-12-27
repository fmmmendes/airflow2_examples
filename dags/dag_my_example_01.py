from email.mime import base
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
# from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
# from airflow.exceptions import AirflowSensorTimeout
# from airflow.models import Variable
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
# from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
# from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
# from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator
from google.cloud import storage
import google.cloud.aiplatform as aip
from airflow.macros import ds_format


tomorrow_date = """{{ macros.ds_format(tomorrow_ds, "%Y-%m-%d", "%Y%m%d_%H") }}"""
di_end = "{{ data_interval_end }}"  # 2022-12-19T05:00:00+00:00
di_end_date = str(di_end.strip("%Y-%m-%d"))
di_end_date_format = """{{ macros.ds_format(data_interval_end | ds, "%Y-%m-%d", "%d/%m/%Y") }}"""
di_end_date_alt = "{{ data_interval_end | ds }}"
tomorrow_date_nodash = "{{tomorrow_ds_nodash}}"

folder = tomorrow_date
hour = tomorrow_date
timestamp = f"{hour}-00-00"
fn_user = f"user-{timestamp}.csv"
fn_object = f"catalog-object-{timestamp}.csv"
local_filepath_user = f"interaction_studio_upload/{folder}/{fn_user}"
local_filepath_catalog_object = f"interaction_studio_upload/{folder}/{fn_object}"

FILE_NAME = f"Data_Extract_{tomorrow_date_nodash}.csv"
PROCESSED_BUCKET_DIR = "processed/{{tomorrow_ds}}"

VAR1="This is tomorrow date: "
    
def generate_aux_data(**kwargs):
    
    date_ymd_nodash = kwargs['date_ymd_nodash']
    
    print(VAR1 + "_" + date_ymd_nodash)
    
    print("Check all references on this link: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html")
    
    return True


with models.DAG(
    "dag_my_example_01",
    schedule_interval='0 5 * * *',
    start_date=pendulum.datetime(2022,11, 20, tz="UTC"),
    catchup=False,
    tags=['my_example']
) as dag:

    
    generate_aux_data = PythonOperator(
        task_id="generate_aux_data",
        python_callable=generate_aux_data,
        op_kwargs=
        {   
            "date_ymd_nodash" : "{{ ds }}",
            "dag_run_start_date":"{{ dag_run.start_date }}",
            "tomorrow_date":tomorrow_date,
            "di_end":di_end,
            "di_end_date":di_end_date,
            "di_end_date_format":di_end_date_format,
            "di_end_date_alt":di_end_date_alt,
            "tomorrow_date_nodash":tomorrow_date_nodash,
            "PROCESSED_BUCKET_DIR":PROCESSED_BUCKET_DIR,
            "folder":folder,
            "hour":hour,
            "timestamp":timestamp,
            "fn_user":fn_user,
            "fn_object":fn_object,
            "local_filepath_user": local_filepath_user,
            "local_filepath_catalog_object":local_filepath_catalog_object,
            "FILE_NAME":FILE_NAME
        },
        retries=2,
        dag=dag
    )


generate_aux_data