#!/usr/bin/env python
# coding: utf-8
"""This script defines methods to write JSON data into InfluxDB."""

import shutil
import time
import datetime
import os
import json
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import pandas as pd
import numpy as np

# JSON field values
TYPE_PARAM_NAME = "type"
USER_PARAM_NAME = "user"
DEVICE_PARAM_NAME = "device_address"

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


def write_file_to_influxdb(file, path_to_data_test_directory, df_client):
    """
    Function writing JSON file to influxDB

    Arguments
    ---------
    file - JSON file to convert and write to InfluxDB
    path_to_data_test_directory - path for reading the JSON file
    get_tmstp - Option to extract all the timestamps of the JSON file

    Returns
    ---------
    write_success (Boolean) - Result of the write process
    """
    write_success = True

    # Open Json file
    try:
        with open(path_to_data_test_directory + file) as json_file:
            json_data = json.load(json_file)
    except:
        print("Impossible to open file : {}".format(file))
        write_success = False
        return write_success

    # Get tags from file
    measurement = json_data[TYPE_PARAM_NAME]
    tags = {USER_PARAM_NAME: json_data[USER_PARAM_NAME],
            DEVICE_PARAM_NAME: json_data[DEVICE_PARAM_NAME]}

    try:
        # Convert json to pandas Dataframe
        if measurement == "MotionAccelerometer":
            data_to_write = convert_acm_json_to_df(json_data)
        elif measurement == "RrInterval":
            data_to_write = convert_rri_json_to_df(json_data)
        elif measurement == "MotionGyroscope":
            data_to_write = convert_gyro_json_to_df(json_data)
    except:
        print("Impossible to convert file to Dataframe.")
        write_success = False
        return write_success

    # Checking if index of data is unique to avoid overwritten points in InfluxDB
    is_index_unique = data_to_write.index.is_unique
    if not is_index_unique:
        data_to_write = create_df_with_unique_index(data_to_write)

    # write to InfluxDB
    try:
        df_client.write_points(data_to_write, measurement=measurement, tags=tags, protocol="json")
    except:
        print("Impossible to write file to influxDB")
        write_success = False

    return write_success


def move_file_processed(file, write_success, path_to_read_directory, path_for_written_files,
                        path_for_problem_files):
    """
    Function dealing with the JSON file once it is processed

    Arguments
    ---------
    file - JSON file processed
    write_success - result of the writing process to influxDB
    path_to_read_directory - directory path where are JSON files
    path_for_written_files - directory where files writen in influxDB are moved
    path_for_problem_files - directory where files not correctly writen in influxDB are moved

    """
    if write_success:
        # move file when write is done in influxdb
        shutil.copy(src=path_to_read_directory + file,
                    dst=path_for_written_files + file)
        os.remove(path=path_to_read_directory + file)
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
    """
    Process all files in the read directory to write them to influxDB.

    Arguments
    ---------
    path_to_read_directory - path from which we read JSON files to write into influxDB.
    path_for_written_files - path where we move correctly written files.
    path_for_problems_files - path where we move files for which write proccess failed.
    df_client - Dataframe InfluxDB Client
    print_logs - Option to print some logs informations about process.
    """
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
    list_files_generator = (file for file in list_files)
    start_time = time.time()
    for json_file in list_files_generator:
        is_writen = write_file_to_influxdb(json_file, path_to_read_directory, df_client)
        #move_file_processed(json_file, is_writen, path_to_read_directory, path_for_written_files,
        #                    path_for_problems_files)

        if print_logs:
            # print logs
            file_processed_timestamp = str(datetime.datetime.now())
            log = "[" + file_processed_timestamp + "]" + " : " + json_file + " processed"
            print(log)

    end_time = time.time()
    if print_logs:
        timestamp = str(datetime.datetime.now())
        total_time = round(end_time - start_time, 3)
        log = "[" + timestamp + "]" + " : " + "Writing process Done in {} s.".format(total_time)
        print(log)


if __name__ == "__main__":

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

    # see InfluxDB Python API for more informations
    # https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
    CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)
    # create_database
    #CLIENT.create_database(DB_NAME)
    # Create influxDB dataframe client
    DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                                database=DB_NAME)
    print("[Client created]")

    execute_write_pipeline(PATH_TO_READ_DIRECTORY, PATH_FOR_WRITTEN_FILES, PATH_FOR_PROBLEMS_FILES,
                           DF_CLIENT, True)
