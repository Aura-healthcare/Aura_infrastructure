#!/usr/bin/env python
# coding: utf-8

from datetime import datetime, timedelta

# Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# own functions
import shutil
import time
import os
import json
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import pandas as pd
import numpy as np


# ---------------- JSON TO DATAFRAME CONVERSION ---------------- #

def convert_acm_json_to_df(acm_json):
    """
    Function converting accelerometer JSON data to a pandas Dataframe

    Arguments
    ---------
    acm_json - Accelerometer JSON file sent from Web-socket

    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    data_to_convert = list(map(lambda x: x.split(" "), acm_json["data"]))

    columns = ["timestamp", "x_acm", "y_acm", "z_acm", "sensibility"]
    df_to_write = pd.DataFrame(data_to_convert, columns=columns).set_index("timestamp")

    # Convert string to numeric values
    df_to_write[["x_acm", "y_acm", "z_acm"]] = df_to_write[["x_acm", "y_acm", "z_acm"]].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


def convert_rri_json_to_df(rri_json):
    """
    Function converting RrInterval JSON data to a pandas Dataframe

    Arguments
    ---------
    acm_json - RrInterval JSON file sent from Web-socket

    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    data_to_convert = list(map(lambda x: x.split(" "), rri_json["data"]))

    columns = ["timestamp", "RrInterval"]
    df_to_write = pd.DataFrame(data_to_convert, columns=columns).set_index("timestamp")

    # Convert string to numeric values
    df_to_write["RrInterval"] = df_to_write["RrInterval"].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


def convert_gyro_json_to_df(gyro_json):
    """
    Function converting gyroscope JSON data to a pandas Dataframe

    Arguments
    ---------
    acm_json - gyroscope JSON file sent from Web-socket

    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    data_to_convert = list(map(lambda x: x.split(" "), gyro_json["data"]))

    columns = ["timestamp", "x_gyro", "y_gyro", "z_gyro"]
    df_to_write = pd.DataFrame(data_to_convert, columns=columns).set_index("timestamp")

    # Convert string to numeric values
    df_to_write[["x_gyro", "y_gyro", "z_gyro"]] = df_to_write[["x_gyro", "y_gyro", "z_gyro"]].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


# ---------------- INFLUXDB INJECTOR ---------------- #

def create_influxdb_client(host="localhost", port=8086, user="root", password="root",
                           db_name="test_db"):
    """
    Function creating an influxDB client using the Python API

    Arguments
    ---------
    host (str) – hostname to connect to InfluxDB, defaults to ‘localhost’
    port (int) – port to connect to InfluxDB, defaults to 8086
    username (str) – user to connect, defaults to ‘root’
    password (str) – password of the user, defaults to ‘root’
    db_name (str) – database name to connect to, defaults to None

    Returns
    ---------
    client - InfluxDB Client
    """
    client = InfluxDBClient(host, port, user, password, db_name)
    print("[Creation client succeed]")
    return client


def create_influxdb_df_client(host="localhost", port=8086, user="root", password="root",
                              db_name="test"):
    """
    Function creating an influxDB Dataframe client using the Python API

    Arguments
    ---------
    host (str) – hostname to connect to InfluxDB, defaults to ‘localhost’
    port (int) – port to connect to InfluxDB, defaults to 8086
    username (str) – user to connect, defaults to ‘root’
    password (str) – password of the user, defaults to ‘root’
    db_name (str) – database name to connect to, defaults to None

    Returns
    ---------
    df_client - Dataframe InfluxDB Client
    """
    df_client = DataFrameClient(host, port, user, password, db_name)
    print("[Creation df_client succeed]")
    return df_client


def create_database(client, db_name="test_db"):
    """
    Function creating an influxDB Database

    Arguments
    ---------
    client - InfluxDB Client
    db_name (str) – database name to create

    Returns
    ---------
    None
    """
    client.create_database(db_name)
    print("[Creation db succeed]")


def query_client(client, influx_query):
    """
    Function querying influxDB

    Arguments
    ---------
    client - InfluxDB Client
    influx_query - influxDB Query to perform

    Returns
    ---------
    result - result of the query
    """
    result = client.query(influx_query)
    return result


# ---------------- PROCESSING FILES ---------------- #


def create_df_with_unique_index(data_to_write):
    """
    Function creating a new Dataframe with a unique index

    Arguments
    ---------
    data_to_write - data to inject in influxDB

    Returns
    ---------
    data_with_unique_index - pandas Dataframe ith unique index to avoid overwritten points
    in influxDB
    """
    # Checking if index of data is unique to avoid overwritten points in InfluxDB
    is_index_unique = data_to_write.index.is_unique
    while not is_index_unique:
        data_to_write.index = data_to_write.index.where(~data_to_write.index.duplicated(),
                                                        data_to_write.index + pd.to_timedelta(123456, unit='ns'))
        data_to_write = data_to_write.sort_index()
        is_index_unique = data_to_write.index.is_unique
    return data_to_write


def write_file_to_influxdb(file, path_to_data_test_directory, df_client, get_tmstp=False):
    """
    Function writing JSON file to influxDB

    Arguments
    ---------
    file - JSON file to convert and write to InfluxDB
    path_to_data_test_directory - path for reading the JSON file
    get_tmstp - Option to extract all the timestamps of the JSON file

    Returns
    ---------
    write_result (Boolean) - Result of the write process
    """
    # Open Json file
    with open(path_to_data_test_directory + file) as jsonfile:
        json_data = json.load(jsonfile)

    # Get tags from file
    measurement = json_data["type"]
    tags = {"user": json_data["user"], "device_address": json_data["device_address"]}

    write_result = True

    try:
        # Convert json to pandas Dataframe
        if "MotionAccelerometer" in file:
            data_to_write = convert_acm_json_to_df(json_data)
        elif "RrInterval" in file:
            data_to_write = convert_rri_json_to_df(json_data)
        elif "MotionGyroscope" in file:
            data_to_write = convert_gyro_json_to_df(json_data)
    except:
        print("Impossible to convert file to Dataframe.")
        write_result = False
        return write_result

    # Checking if index of data is unique to avoid overwritten points in InfluxDB
    is_index_unique = data_to_write.index.is_unique
    if not is_index_unique:
        data_to_write = create_df_with_unique_index(data_to_write)

    # write to InfluxDB
    try:
        df_client.write_points(data_to_write, measurement=measurement, tags=tags, protocol="json")
    except:
        write_result = False

    # To test if all points are writen in influxDB
    if get_tmstp:
        timestamp_list = data_to_write.index
        return write_result, timestamp_list

    return write_result


def move_file_processed(file, write_result, path_to_read_directory, path_for_written_files,
                        path_for_problem_files):
    """
    Function dealing with the JSON file once it is processed

    Arguments
    ---------
    file - JSON file processed
    write_result - result of the writing process to influxDB
    path_to_read_directory - directory path where are JSON files
    path_for_written_files - directory where files writen in influxDB are moved
    path_for_problem_files - directory where files not correctly writen in influxDB are moved
    """
    if write_result:
        # move file when write is done in influxdb
        shutil.move(src=path_to_read_directory + file,
                    dst=path_for_written_files + file)
        #os.remove(path=path_to_read_directory + file)
    else:
        shutil.move(src=path_to_read_directory + file,
                    dst=path_for_problem_files + file)


def test_influxdb(df_client, nb_points):
    """
    Function to test influxDB's ability to write points

    Arguments
    ---------
    df_client - Dataframe InfluxDB Client
    nb_points - number of points to write to influxDB in a one time try

    """
    # Get tags for test df
    measurement = "test"
    tags = {"user": "robin", "device_address": "iphone"}

    for i in range(10, 30):
        # Test resistance of InfluxDB
        nb_points = nb_points
        dates = pd.date_range("201808" + str(i), periods=nb_points, freq="S")
        columns = ["RrInterval_test"]
        data_to_write = pd.DataFrame(np.random.randint(500, 1000, nb_points),
                                     index=dates, columns=columns)

        df_client.write_points(data_to_write, measurement=measurement, tags=tags, protocol="json")


def execute_write_pipeline(path_to_read_directory, path_for_written_files, path_for_problems_files,
                           df_client, print_logs=False):

    # List files to process
    list_files = os.listdir(path_to_read_directory)
    if ".DS_Store" in list_files:
        list_files.remove(".DS_Store")
    if print_logs:
        print("There are currently {} files.".format(len(list_files)))

    # Creating directory for processed files
    if not os.path.exists(path_for_written_files):
        os.makedirs(path_for_written_files)
    if not os.path.exists(path_for_problems_files):
        os.makedirs(path_for_problems_files)

    # Processing & writing files to influx and cleaning directory

    T0 = time.time()
    for json_file in list_files:
        is_writen = write_file_to_influxdb(json_file, path_to_read_directory, df_client)
        move_file_processed(json_file, is_writen, path_to_read_directory, path_for_written_files,
                            path_for_problems_files)

        if print_logs:
            # print logs
            file_processed_timestamp = str(datetime.now())
            log = "[" + file_processed_timestamp + "]" + " : " + json_file + " processed"
            print(log)

    T1 = time.time()
    if print_logs:
        timestamp = str(datetime.now())
        log = "[" + timestamp + "]" + " : " + "Writing process Done in {} s.".format(round(T1 - T0, 3))
        print(log)


# Multiple path for files processing
PATH_TO_READ_DIRECTORY = "{{ airflow_data_input_location_in_container }}"
PATH_FOR_WRITTEN_FILES = "{{ airflow_data_output_success_location_in_container }}"
PATH_FOR_PROBLEMS_FILES = "{{ airflow_data_output_failed_location_in_container }}"

# Creation of client & Influx Database
DB_NAME = "physio_signals"
CLIENT = create_influxdb_client(host="influxdb", port=8086, user="root", password="root", db_name=DB_NAME)
#create_database(CLIENT, db_name=DB_NAME)
DF_CLIENT = create_influxdb_df_client(host="influxdb", port=8086, user="root", password="root", db_name=DB_NAME)

default_args = {
    'owner': 'Robin_Champseix',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 2, 10, 40),
    'email': ['rchampseix@octo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('raw_data_injector', default_args=default_args, schedule_interval="*/5 * * * *")

write_data = PythonOperator(task_id='write_data',
                            python_callable=execute_write_pipeline,
                            op_kwargs={"path_to_read_directory": PATH_TO_READ_DIRECTORY,
                                       "path_for_written_files": PATH_FOR_WRITTEN_FILES,
                                       "path_for_problems_files": PATH_FOR_PROBLEMS_FILES,
                                       "df_client": DF_CLIENT,
                                       "print_logs": True},
                            dag=dag)
write_data
