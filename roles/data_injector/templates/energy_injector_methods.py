#!/usr/bin/env python
# coding: utf-8
"""This script defines methods to compute features from InfluxDB Data"""

import math
import json
from typing import List
from datetime import timezone
import datetime
import configparser
import numpy as np
import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient

# JSON field values
TYPE_PARAM_NAME = "type"
USER_PARAM_NAME = "user"
DEVICE_PARAM_NAME = "device_address"

ACCELEROMETER_MEASUREMENT_NAME = "MotionAccelerometer"


def get_user_list(client, measurement: str = "RrInterval") -> List[str]:
    """
    Get the list of all distinct user in the influxDB database.

    Arguments
    ---------
    client: Client object
        influxDB client to connect to database.
    measurment : str
        Type of measurement to user list of.

    :return usr_list: list of all distinct user in database.
    """
    # Get list of all users
    influx_query = "SHOW TAG VALUES WITH KEY = \"user\""
    query_result = client.query(influx_query)
    user_values_dict = list(query_result.get_points(measurement=measurement))

    usr_list = []
    for elt in user_values_dict:
        usr_list.append(elt["value"])

    return usr_list


def transform_acm_result_set_into_dataframe(result_set: str, tags: dict) -> pd.DataFrame:
    """
    TODO

    :param result_set:
    :param measurement:
    :param tags:
    :return:
    """
    raw_acm_data_list = list(result_set.get_points(measurement=ACCELEROMETER_MEASUREMENT_NAME, tags=tags))
    raw_acm_dataframe = pd.DataFrame(raw_acm_data_list)[["time", "x_acm", "y_acm", "z_acm"]]
    raw_acm_dataframe["time"] = pd.to_datetime(raw_acm_dataframe["time"])
    raw_acm_dataframe.index = raw_acm_dataframe["time"]
    return raw_acm_dataframe.dropna()


def create_energy_data_points(acm_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    TODO

    :param acm_dataframe:
    :return:
    """
    boolean_mask = acm_dataframe["time"].diff(periods=1) < "00:00:00.5"
    consecutive_differences_dataframe = acm_dataframe.diff(periods=1)[boolean_mask].drop(["time"], axis=1)
    squared_differences_dataframe = consecutive_differences_dataframe ** 2

    triaxial_sum_series = squared_differences_dataframe.apply(sum, axis=1)
    triaxial_sqrt_dataframe = triaxial_sum_series.apply(np.sqrt).to_frame()
    triaxial_sqrt_dataframe = triaxial_sqrt_dataframe.rename(columns={0: "acm_triaxial_sqrt_diff_sum"})
    acm_dataframe_index = acm_dataframe[boolean_mask]["time"]
    triaxial_sqrt_dataframe.index = acm_dataframe_index
    triaxial_sqrt_dataframe.index.name = "timestamp"

    return triaxial_sqrt_dataframe


def get_difference_between_current_time_and_json_file_timestamp(timestamp: str):
    timestamp = pd.to_datetime(timestamp)
    current_timestamp = datetime.datetime.now()
    time_delta = current_timestamp - timestamp

    # x1000 to get ms value
    unix_timestamp = timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000

    return time_delta.days, unix_timestamp


def chunk_and_write_dataframe(dataframe_to_write: pd.DataFrame, measurement: str,
                              user_id: str, df_client) -> bool:
    """

    :param dataframe_to_write:
    :param measurement:
    :param user_id:
    :return:
    """
    # Write new features into influxdb - Chunk dataframe for influxdb performance issues
    chunk_nb = math.ceil(len(dataframe_to_write) / 5000)
    for chunk in np.array_split(dataframe_to_write, chunk_nb):
        tags = {USER_PARAM_NAME: user_id}
        df_client.write_points(chunk, measurement=measurement, tags=tags, protocol="json")
    return True


def extract_data_from_influxdb_query(measurement: str, user_id: str,
                                     start_time: str, end_time: str):
    # Extract raw data from InfluxDB for D-day
    query = "SELECT * FROM {} WHERE \"user\" = '{}' and time > now() - {} and time < now() - {}".format(measurement,
                                                                                                        user_id,
                                                                                                        start_time,
                                                                                                        end_time)
    extracted_data_result_set = CLIENT.query(query)
    return extracted_data_result_set


def write_user_energy_data(user_id: str, last_timestamp: str, df_client) -> bool:
    """
    TODO

    :param user_id:
    :param last_timestamp:
    :return:
    """
    print("[Creation of features] user {}".format(user_id))

    day_range_to_query, last_unix_timestamp = get_difference_between_current_time_and_json_file_timestamp(last_timestamp)

    # Extract raw data from InfluxDB
    start = str(int(last_unix_timestamp)) + "ms"
    end = str(day_range_to_query) + "d"
    extracted_data_result_set = extract_data_from_influxdb_query(ACCELEROMETER_MEASUREMENT_NAME,
                                                                 user_id, start, end)
    # Transform InfluxDB ResultSet in pandas Dataframe
    tags = {"user": user_id}
    try:
        raw_acm_dataframe = transform_acm_result_set_into_dataframe(extracted_data_result_set, tags)
        # Create triaxial sum data points
        triaxial_sum_dataframe = create_energy_data_points(raw_acm_dataframe)
        chunk_and_write_dataframe(triaxial_sum_dataframe, ACCELEROMETER_MEASUREMENT_NAME, user_id, df_client)
    except KeyError:
        pass
    for day in reversed(range(day_range_to_query)):
        # Extract raw data from InfluxDB for D-day
        start, end = str(day+1) + "d", str(day) + "d"
        extracted_data_result_set = extract_data_from_influxdb_query(ACCELEROMETER_MEASUREMENT_NAME,
                                                                     user_id, start, end)

        # Transform InfluxDB ResultSet in pandas Dataframe
        tags = {"user": user_id}
        try:
            raw_acm_dataframe = transform_acm_result_set_into_dataframe(extracted_data_result_set, tags)
        except KeyError:
            # If there is no data for D-day, skip processing
            continue

        # Create triaxial sum data points
        triaxial_sum_dataframe = create_energy_data_points(raw_acm_dataframe)

        chunk_and_write_dataframe(triaxial_sum_dataframe, ACCELEROMETER_MEASUREMENT_NAME, user_id, df_client)

    last_processed_data_timestamp = triaxial_sum_dataframe.tail(1).index.values[0]
    print("[Writing process Done]")

    return last_processed_data_timestamp


def extract_user_timestamp_json(file_name: str, user_list: list):
    """
    TODO
    :param file_name:
    :return:
    """
    try:
        with open(file_name) as json_file:
            json_timestamp_by_user = json.load(json_file)
        for user in user_list:
            if json_timestamp_by_user.get(user) is None:
                json_timestamp_by_user[user] = "2018-01-01T00:00:00"

    except FileNotFoundError:
        json_timestamp_by_user = {}
        # Initialize timestamp
        for user in user_list:
            json_timestamp_by_user[user] = "2018-01-01T00:00:00"
    return json_timestamp_by_user


def update_user_timestamp(updated_timestamp, user, data, path_file):
    """
    TODO
    :param updated_timestamp:
    :param user:
    :param data:
    :param path_file:
    :return:
    """
    data[user] = str(updated_timestamp)
    with open(path_file, 'w') as processed_data_timestamp:
        json.dump(timestamp_json_data, processed_data_timestamp)


def write_energy_and_update_timestamp(user_id: str, timestamp_json_data, json_data_file_path, df_client):
    user_last_energy_timestamp = timestamp_json_data[user_id]

    last_written_data_point_timestamp = write_user_energy_data(user, user_last_energy_timestamp, df_client)
    update_user_timestamp(last_written_data_point_timestamp, user_id, timestamp_json_data, json_data_file_path)


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('config.conf')

    # Useful Influx client constants
    influxdb_client_constants = config["Influxdb Client"]
    DB_NAME = influxdb_client_constants["database_name"]
    HOST = influxdb_client_constants["host"]
    PORT = int(influxdb_client_constants["port"])
    USER = influxdb_client_constants["user"]
    PASSWORD = influxdb_client_constants["password"]

    # see InfluxDB Python API for more information
    # https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
    CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD, database=DB_NAME)
    DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD, database=DB_NAME)
    print("[Client created]")

    MEASUREMENT = "MotionAccelerometer"
    user_list = get_user_list(CLIENT, MEASUREMENT)

    JSON_DATA_FILE_PATH = "processed_data_timestamp.json"
    timestamp_json_data = extract_user_timestamp_json(JSON_DATA_FILE_PATH, user_list)

    for user in user_list:
        write_energy_and_update_timestamp(user, timestamp_json_data, JSON_DATA_FILE_PATH, DF_CLIENT)
