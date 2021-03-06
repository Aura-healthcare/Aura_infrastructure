#!/usr/bin/env python
# coding: utf-8
"""This script defines Dags to inject data into influxDB via airflow."""

import os
from datetime import datetime, timedelta
import configparser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from energy_injector_methods import (create_and_write_energy_for_user,
                                     get_user_list)

run_path = os.path.dirname(os.path.abspath(__file__))

config = configparser.ConfigParser()
config.read(run_path + '/config.conf')

influxdb_client_constants = config["Influxdb Client"]
DB_NAME = influxdb_client_constants["database_name"]
HOST = influxdb_client_constants["host"]
PORT = int(influxdb_client_constants["port"])
USER = influxdb_client_constants["user"]
PASSWORD = influxdb_client_constants["password"]

motion_acm_constants = config["Motion Accelerometer"]
FIVE_SEC_THRESHOLD = motion_acm_constants["five_sec_threshold"]
ONE_MIN_THRESHOLD = motion_acm_constants["one_min_threshold"]
MAX_SUCCESSIVE_TIME_DIFF = motion_acm_constants["max_successive_time_diff"]
ACCELEROMETER_MEASUREMENT_NAME = "MotionAccelerometer"

# see InfluxDB Python API for more information
# https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                        database=DB_NAME)
DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)
print("[Client created]")

user_list = get_user_list(CLIENT)
print("Users : " + str(user_list))

airflow_config = config["Airflow"]
default_args = {
    'owner': airflow_config["owner"],
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 2, 14, 5),
    'email': airflow_config["email"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('energy_data_injector', default_args=default_args, schedule_interval="@daily")

for user in user_list:
    write_energy_data = PythonOperator(task_id='create_and_write_energy_for_user',
                                       python_callable=create_and_write_energy_for_user,
                                       op_kwargs={"user_id": user,
                                                  "client": CLIENT,
                                                  "df_client": DF_CLIENT,
                                                  "accelerometer_measurement_name": ACCELEROMETER_MEASUREMENT_NAME,
                                                  "five_sec_threshold": FIVE_SEC_THRESHOLD,
                                                  "one_min_threshold": ONE_MIN_THRESHOLD,
                                                  "max_successive_time_diff": MAX_SUCCESSIVE_TIME_DIFF,
                                                  },
                                       dag=dag)
    write_energy_data