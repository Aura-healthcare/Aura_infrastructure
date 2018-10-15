#!/usr/bin/env python
# coding: utf-8
"""This script defines Dags to inject data into influxDB via airflow."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from influxdb_raw_data_injector import execute_write_pipeline

# Multiple path for files processing
PATH_TO_READ_DIRECTORY = "{{ airflow_data_input_location_in_container }}"
PATH_FOR_WRITTEN_FILES = "{{ airflow_data_output_success_location_in_container }}"
PATH_FOR_PROBLEMS_FILES = "{{ airflow_data_output_failed_location_in_container }}"

# Useful Influx client constants
DB_NAME = "{{ aura_physio_data_db_name }}"
HOST = "{{ aura_time_series_db_container }}"
PORT = {{ aura_time_series_db_port }}
USER = "root"
PASSWORD = "root"

# see InfluxDB Python API for more information
# https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                        database=DB_NAME)

# Create influxDB dataframe client
DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)
print("[Client created]")

default_args = {
    'owner': 'Robin_Champseix',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 2, 14, 5),
    'email': ['rchampseix@octo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# Crontab expression = every day, every hour between 17h and 00 h
# 0,17,18,19,20,21,22,23 ? * * *
dag = DAG('raw_data_injector', default_args=default_args,
          schedule_interval="@hourly")

write_data = PythonOperator(task_id='write_data_into_influxdb',
                            python_callable=execute_write_pipeline,
                            op_kwargs={"path_to_read_directory": PATH_TO_READ_DIRECTORY,
                                       "path_for_written_files": PATH_FOR_WRITTEN_FILES,
                                       "path_for_problems_files": PATH_FOR_PROBLEMS_FILES,
                                       "df_client": DF_CLIENT,
                                       "print_logs": True},
                            dag=dag)

write_data
