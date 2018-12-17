#!/usr/bin/env python
# coding: utf-8
"""This script defines methods to write JSON data into InfluxDB."""

import shutil
import math
import datetime
import os
import glob
import configparser
import json
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import pandas as pd
import numpy as np
import math

# JSON field values
TYPE_PARAM_NAME = "type"
USER_PARAM_NAME = "user"
DEVICE_PARAM_NAME = "device_address"

# ---------------- JSON TO DATAFRAME CONVERSION ---------------- #


def convert_acm_json_to_df(acm_json: dict) -> pd.DataFrame:
    """
    Function converting accelerometer JSON data to pandas Dataframe.

    Arguments
    ---------
    acm_json - Accelerometer JSON file sent from Web-socket

    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    extracted_data_from_json_file = list(map(lambda x: x.split(" "), acm_json["data"]))

    columns = ["timestamp", "x_acm", "y_acm", "z_acm", "sensibility"]
    df_to_write = pd.DataFrame(extracted_data_from_json_file, columns=columns).set_index("timestamp")

    # Convert string to numeric values
    df_to_write[["x_acm", "y_acm", "z_acm"]] = df_to_write[["x_acm", "y_acm", "z_acm"]].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


def convert_rri_json_to_df(rri_json) -> pd.DataFrame:
    """
    Function converting RrInterval JSON data to pandas Dataframe.

    Arguments
    ---------
    acm_json - RrInterval JSON file sent from Web-socket

    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    extracted_data_from_json_file = list(map(lambda x: x.split(" "), rri_json["data"]))

    columns = ["timestamp", "RrInterval"]
    df_to_write = pd.DataFrame(extracted_data_from_json_file, columns=columns).set_index("timestamp")

    # Convert string to numeric values
    df_to_write["RrInterval"] = df_to_write["RrInterval"].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


def convert_gyro_json_to_df(gyro_json) -> pd.DataFrame:
    """
    Function converting gyroscope JSON data to pandas Dataframe.

    Arguments
    ---------
    acm_json - gyroscope JSON file sent from Web-socket

    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    extracted_data_from_json_file = list(map(lambda x: x.split(" "), gyro_json["data"]))

    columns = ["timestamp", "x_gyro", "y_gyro", "z_gyro"]
    df_to_write = pd.DataFrame(extracted_data_from_json_file, columns=columns).set_index("timestamp")

    # Convert string to numeric values
    df_to_write[["x_gyro", "y_gyro", "z_gyro"]] = df_to_write[["x_gyro", "y_gyro", "z_gyro"]].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


# ---------------- PROCESSING FILES ---------------- #


def create_df_with_unique_index(data_to_write: pd.DataFrame,
                                time_delta_to_add: int = 123456) -> pd.DataFrame:
    """
    Function creating a new Dataframe with a unique index to avoid points overwrite in time series DB.

    Arguments
    ---------
    data_to_write - data to inject in influxDB
    time_delta_to_add - timedelta to add, in ns, to avoid having 2 points with same timestamp index

    Returns
    ---------
    data_with_unique_index - pandas Dataframe with unique index to avoid overwritten points
    in influxDB
    """
    # Checking if index of data is unique to avoid overwritten points in InfluxDB
    is_index_unique = data_to_write.index.is_unique
    while not is_index_unique:
        data_to_write.index = data_to_write.index.where(~data_to_write.index.duplicated(),
                                                        data_to_write.index + pd.to_timedelta(time_delta_to_add,
                                                                                              unit='ns'))
        data_to_write = data_to_write.sort_index()
        is_index_unique = data_to_write.index.is_unique
    return data_to_write


def write_file_to_influxdb(file: str, path_to_files: str, df_client) -> bool:
    """
    Function writing JSON file to influxDB.

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
        with open(path_to_files + file) as json_file:
            json_data = json.load(json_file)
        # Get tags from file
        measurement = json_data[TYPE_PARAM_NAME]
        tags = {USER_PARAM_NAME: json_data[USER_PARAM_NAME],
                DEVICE_PARAM_NAME: json_data[DEVICE_PARAM_NAME]}
    except:
        print("Impossible to open file.")
        write_success = False
        return write_success

    try:
        # Convert json to pandas Dataframe
        if measurement == "MotionAccelerometer":
            data_to_write = convert_acm_json_to_df(json_data)
        else:
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


def move_processed_file(file: str, write_success: bool, path_to_read_directory: str,
                        path_for_written_files: str, path_for_problem_files: str):
    """
    Function dealing with the JSON file once it is processed.

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
        shutil.move(src=path_to_read_directory + file,
                    dst=path_for_written_files + file)
    else:
        shutil.move(src=path_to_read_directory + file,
                    dst=path_for_problem_files + file)


def create_files_by_user_dict(files_list: list) -> dict:
    """
    Create a dictionary containing the corresponding list of RR-inteval files for each user.

    Arguments
    ---------
    files_list - list of files to sort

    Returns
    ---------
    files_by_user_dict - sorted dictionary

    ex :
    files_by_user_dict = {
            'user_1': ["file_1", "file_2", "file_3"],
            'user_2': ["file_4", "file_5"],
            'user_3': ["file_6", "file_7", "file_8"]
    }
    """
    # Create sorted user list
    user_list = list(set(map(lambda x: x.split("/")[-1].split("_")[0], files_list)))
    user_list.sort()

    files_by_user_dict = dict()
    file_list_for_a_user = []
    try:
        current_user = user_list[0]
    except IndexError:
        return dict()

    for filename in files_list:
        if current_user in filename:
            file_list_for_a_user.append(filename)
        else:
            files_by_user_dict[current_user] = file_list_for_a_user
            current_user = filename.split("_")[0]
            file_list_for_a_user = [filename]

    # Add list of files for last user in dictionary
    files_by_user_dict[current_user] = file_list_for_a_user
    return files_by_user_dict


def concat_rrinterval_files_into_single_dataframe(files_list: list) -> pd.DataFrame:
    """
    Concatenate JSON files content into a single pandas DataFrame.

    Arguments
    ---------
    files_list - list of files to sort

    Returns
    ---------
    concatenated_rr_interval_dataframe - resulting pandas DataFrame
    """
    dataframe_list = []
    for file in files_list:
        # Open Json file
        with open(file) as json_file:
            json_data = json.load(json_file)

        # Get tags from file
        measurement = json_data[TYPE_PARAM_NAME]

        # Extract data and create dataframe from JSON file
        if measurement == "RrInterval":
            rr_interval_dataframe = convert_rri_json_to_df(json_data)
            dataframe_list.append(rr_interval_dataframe)

    # Concat list of dataframe
    concatenated_rr_interval_dataframe = pd.concat(dataframe_list)
    return concatenated_rr_interval_dataframe


def create_corrected_timestamp_list(concatenated_rr_interval_dataframe: pd.DataFrame) -> list:
    """
    Create a corrected timestamp based on cumulative sum of RR-intervals values.

    Arguments
    ---------
    concatenated_df - pandas DataFrame containing all data of a specific user

    Returns
    ---------
    corrected_timestamp_list - Corrected timestamp generated
    """
    rri_list = concatenated_rr_interval_dataframe["RrInterval"].values
    polar_index = concatenated_rr_interval_dataframe.index

    current_timestamp = polar_index[0]
    next_timestamp = polar_index[1]

    # Set the first timestamp to be the first timestamp of the polar
    corrected_timestamp_list = [current_timestamp]

    for i in range(1, len(polar_index) - 1):
        next_corrected_timestamp = get_next_timestamp(next_timestamp, current_timestamp,
                                                      corrected_timestamp_list[-1], rri_list[i])
        corrected_timestamp_list.append(next_corrected_timestamp)

        # Update next timestamps to compute time difference
        current_timestamp = polar_index[i]
        next_timestamp = polar_index[i+1]

    # Deal with last timestamp value
    next_corrected_timestamp = get_next_timestamp(next_timestamp, current_timestamp,
                                                  corrected_timestamp_list[-1], rri_list[-1])
    corrected_timestamp_list.append(next_corrected_timestamp)

    return corrected_timestamp_list


def get_next_timestamp(next_timestamp, current_timestamp, last_corrected_timestamp,
                       next_rr_interval: float):
    """

    :param next_timestamp:
    :param current_timestamp:
    :param last_corrected_timestamp:
    :param next_rr_interval:
    :return:
    """
    # Deal with last timestamp value
    time_difference = next_timestamp - current_timestamp
    if abs(time_difference.seconds) < 3:
        next_corrected_timestamp = last_corrected_timestamp + \
                                   datetime.timedelta(milliseconds=np.float64(next_rr_interval))
        return next_corrected_timestamp
    else:
        return next_timestamp


def execute_rri_files_write_pipeline(path_to_read_directory: str, path_for_written_files: str,
                                     path_for_problems_files: str, df_client, verbose: bool = False):
    """
    Process all files in the read directory to write them to influxDB.

    Arguments
    ---------
    path_to_read_directory - path from which we read JSON files to write into influxDB.
    path_for_written_files - path where we move correctly written files.
    path_for_problems_files - path where we move files for which write proccess failed.
    df_client - Dataframe InfluxDB Client
    verbose - Option to print some logs informations about process.
    """
    # Get files list containing RR-Interval in directory
    rri_files_list = glob.glob(path_to_read_directory + "*RrInterval*")
    rri_files_list.sort()
    if verbose:
        print("There are currently {} files.".format(len(rri_files_list)))

    # group and sort files by user
    sorted_rri_files_dict = create_files_by_user_dict(rri_files_list)

    # Creating directory for processed file
    # ---------- DELETE IN PRODUCTION MODE ---------- #
    if not os.path.exists(path_for_written_files):
        os.makedirs(path_for_written_files)
    if not os.path.exists(path_for_problems_files):
        os.makedirs(path_for_problems_files)

    for user in sorted_rri_files_dict.keys():
        write_success = True

        user_rri_files = sorted_rri_files_dict[user]

        # concat multiple files of each user
        concatenated_dataframe = concat_rrinterval_files_into_single_dataframe(files_list=user_rri_files)

        # GET raw data count by min
        rri_count_by_min = concatenated_dataframe.resample("1min", label="right").count()
        rri_count_by_min.columns = ["rr_interval_count_by_min"]

        # Create new timestamp
        corrected_timestamp_list = create_corrected_timestamp_list(concatenated_dataframe)
        concatenated_dataframe.index = corrected_timestamp_list
        concatenated_dataframe.index.names = ["timestamp"]

        # Open Json file
        try:
            with open(user_rri_files[0]) as json_file:
                json_data = json.load(json_file)
            tags = {USER_PARAM_NAME: json_data[USER_PARAM_NAME],
                    DEVICE_PARAM_NAME: json_data[DEVICE_PARAM_NAME]}
        except:
            print("Impossible to open file.")
            write_success = False

        # write raw RR-interval count by min to InfluxDB
        try:
            chunk_and_write_dataframe(rri_count_by_min, measurement="RrInterval", tags=tags,
                                      user_id=user, df_client=df_client, batch_size=5000)
        except:
            print("Impossible to write RR-interval raw data count by min to influxDB.")

        print("DF SHAPE : {}".format(concatenated_dataframe.shape))
        # write processed rr_interval data to InfluxDB
        try:
            chunk_and_write_dataframe(concatenated_dataframe, measurement="RrInterval",
                                      user_id=user, df_client=df_client, batch_size=5000)
        except:
            print("Impossible to write file to influxDB")
            write_success = False

        for json_file in user_rri_files:
            move_processed_file(json_file.split("/")[-1], write_success, path_to_read_directory,
                                path_for_written_files, path_for_problems_files)
            if verbose:
                file_processed_timestamp = str(datetime.datetime.now())
                log = "[" + file_processed_timestamp + "]" + " : " + json_file + " processed"
                print(log)


def chunk_and_write_dataframe(dataframe_to_write: pd.DataFrame, measurement: str,
                              user_id: str, df_client, batch_size: int = 5000) -> bool:
    """

    :param dataframe_to_write:
    :param measurement:
    :param user_id:
    :param df_client:
    :param batch_size:
    :return:
    """
    # Chunk dataframe for time series db performance issues
    chunk_nb = math.ceil(len(dataframe_to_write) / batch_size)
    print("There are {} chunks to write.".format(chunk_nb))

    dataframe_chunk_list = np.array_split(dataframe_to_write, chunk_nb)
    # Write each chunk in time series db
    for chunk in dataframe_chunk_list:
        tags = {USER_PARAM_NAME: user_id}
        df_client.write_points(chunk, measurement=measurement, tags=tags, protocol="json")
    return True


def execute_acm_gyro_files_write_pipeline(path_to_read_directory: str, path_for_written_files: str,
                                          path_for_problems_files: str, df_client, verbose=False):
    """
    Process all gyroscope and accelerometer files in the read directory to write them to influxDB.

    Arguments
    ---------
    path_to_read_directory - path from which we read JSON files to write into influxDB.
    path_for_written_files - path where we move correctly written files.
    path_for_problems_files - path where we move files for which write proccess failed.
    df_client - Dataframe InfluxDB Client
    verbose - Option to print some logs informations about process.
    """
    # List files to process
    list_files = os.listdir(path_to_read_directory)
    if verbose:
        print("There are currently {} files.".format(len(list_files)))

    # Creating directory for processed files
    # ---------- DELETE IN PRODUCTION MODE ---------- #
    if not os.path.exists(path_for_written_files):
        os.makedirs(path_for_written_files)
    if not os.path.exists(path_for_problems_files):
        os.makedirs(path_for_problems_files)

    # Processing & writing files to influx and cleaning directory
    list_files_generator = (file for file in list_files)
    for json_file in list_files_generator:
        is_writen = write_file_to_influxdb(json_file, path_to_read_directory, df_client)
        move_processed_file(json_file, is_writen, path_to_read_directory, path_for_written_files,
                            path_for_problems_files)

        if verbose:
            file_processed_timestamp = str(datetime.datetime.now())
            log = "[" + file_processed_timestamp + "]" + " : " + json_file + " processed"
            print(log)


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('config.conf')

    files_processing_paths = config["Paths"]
    PATH_TO_READ_DIRECTORY = files_processing_paths["read_directory"]
    PATH_FOR_WRITTEN_FILES = files_processing_paths["success_files_directory"]
    PATH_FOR_PROBLEMS_FILES = files_processing_paths["failed_files_directory"]

    influxdb_client_constants = config["Influxdb Client"]
    DB_NAME = influxdb_client_constants["database_name"]
    HOST = influxdb_client_constants["host"]
    PORT = int(influxdb_client_constants["port"])
    USER = influxdb_client_constants["user"]
    PASSWORD = influxdb_client_constants["password"]

    # Create influxDB clients - see InfluxDB Python API for more informations
    # https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
    CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)
    DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                                database=DB_NAME)
    print("[Creation Client Success]")

    # Create database
    # CLIENT.create_database(DB_NAME)

    # -------- Write pipeline -------- #
    execute_rri_files_write_pipeline(PATH_TO_READ_DIRECTORY, PATH_FOR_WRITTEN_FILES,
                                     PATH_FOR_PROBLEMS_FILES, DF_CLIENT, True)

    execute_acm_gyro_files_write_pipeline(PATH_TO_READ_DIRECTORY, PATH_FOR_WRITTEN_FILES,
                                          PATH_FOR_PROBLEMS_FILES, DF_CLIENT, True)
