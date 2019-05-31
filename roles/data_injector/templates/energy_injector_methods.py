#!/usr/bin/env python
# coding: utf-8
"""This script defines methods to compute features from InfluxDB Data"""

from typing import List
import math
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

# --------------------- FUNCTIONS TO QUERY INFLUXDB --------------------- #


def get_user_list(client) -> List[str]:
    """
    Get the list of all distinct user in the influxDB database.
    Arguments
    ---------
    client: Client object
        influxDB client to connect to database.
    :return usr_list: list of all distinct user in database.
    """
    # Get list of all users
    influx_query = "SHOW TAG VALUES WITH KEY = \"user\""
    query_result = client.query(influx_query)
    user_values_dict = list(query_result.get_points())

    usr_list = []
    for elt in user_values_dict:
        usr_list.append(elt["value"])

    return list(set(usr_list))


def extract_raw_data_from_influxdb(client, measurement: str, user_id: str, start_time: str, end_time: str):
    """
    TODO
    :param client:
    :param measurement:
    :param user_id:
    :param start_time:
    :param end_time:
    :return extracted_result_set: influxDB object containing extracted data
    """
    # Extract raw data from InfluxDB for D-day
    query = "SELECT * FROM {} WHERE \"user\" = '{}' and time > now() - {} and time < now() - {}".format(measurement,
                                                                                                        user_id,
                                                                                                        start_time,
                                                                                                        end_time)
    extracted_result_set = client.query(query)
    return extracted_result_set


# --------------------- FUNCTIONS TO COMPUTE TIME RANGE TO QUERY --------------------- #


def get_first_timestamp_to_compute_energy(user_id: str, client):
    """
    :param user_id:
    :param client:
    :return:
    """
    query = "SELECT last(\"energy_by_5s\") FROM MotionAccelerometer WHERE \"user\" = '{}'".format(user_id)
    extracted_data_result_set = client.query(query)
    last_energy_timestamp_for_user = list(extracted_data_result_set.get_points())

    if last_energy_timestamp_for_user:
        # Get last timestamp of energy data for user
        first_timestamp_to_compute_energy = last_energy_timestamp_for_user[0]["time"]
        print("Last energy timestamp in time series db: {}".format(first_timestamp_to_compute_energy))
    else:
        # Get first timestamp of Accelerometer data for user
        print("No energy data for user : {}".format(user_id))
        print("[Calculating energy from all MotionAccelerometer data]")
        query = "SELECT first(\"x_acm\") FROM MotionAccelerometer WHERE \"user\" = '{}'".format(user_id)
        extracted_data_result_set = client.query(query)
        first_acm_timestamp_for_user = list(extracted_data_result_set.get_points())
        first_timestamp_to_compute_energy = first_acm_timestamp_for_user[0]["time"]

    return pd.to_datetime(first_timestamp_to_compute_energy, unit="ns")


def get_time_difference_between_now_and_timestamp(timestamp):
    """
    TODO
    :param timestamp:
    :return:
    """
    current_timestamp = datetime.datetime.now()
    time_delta = current_timestamp - timestamp

    return time_delta.days


# --------------------- FUNCTIONS TO COMPUTE ENERGY FROM ACM QUERY RESULT --------------------- #


def transform_acm_result_set_into_dataframe(result_set: str, tags: dict) -> pd.DataFrame:
    """
    TODO
    :param result_set:
    :param tags:
    :return:
    """
    raw_acm_data_list = list(result_set.get_points(measurement=ACCELEROMETER_MEASUREMENT_NAME, tags=tags))
    raw_acm_dataframe = pd.DataFrame(raw_acm_data_list)[["time", "x_acm", "y_acm", "z_acm"]]
    raw_acm_dataframe["time"] = pd.to_datetime(raw_acm_dataframe["time"])
    raw_acm_dataframe.index = raw_acm_dataframe["time"]
    return raw_acm_dataframe.dropna()


def create_energy_dataframe(acm_dataframe: pd.DataFrame, aggregation_count_threshold: int,
                            max_successive_time_diff: str, aggregation_time: str) -> pd.DataFrame:
    """
    TODO
    :param acm_dataframe:
    :param aggregation_count_threshold:
    :param max_successive_time_diff:
    :param aggregation_time:
    :return:
    """
    max_successive_time_diff_boolean_mask = acm_dataframe["time"].diff(periods=1) < max_successive_time_diff
    consecutive_differences_dataframe = acm_dataframe.diff(periods=1)[max_successive_time_diff_boolean_mask].drop(["time"], axis=1)

    squared_differences_dataframe = consecutive_differences_dataframe ** 2
    triaxial_sum_series = squared_differences_dataframe.apply(sum, axis=1)
    triaxial_sqrt_dataframe = triaxial_sum_series.apply(np.sqrt).to_frame()

    acm_dataframe_index = acm_dataframe[max_successive_time_diff_boolean_mask]["time"]
    triaxial_sqrt_dataframe.index = acm_dataframe_index
    triaxial_sqrt_dataframe.index.name = "timestamp"

    count_threshold_boolean_mask = triaxial_sqrt_dataframe.resample(aggregation_time, label="right").count() > aggregation_count_threshold
    energy_dataframe = triaxial_sqrt_dataframe.resample(aggregation_time, label="right").sum()
    energy_dataframe = energy_dataframe[count_threshold_boolean_mask].dropna()
    energy_dataframe = energy_dataframe.rename(columns={0: "energy_by_{}".format(aggregation_time)})

    return energy_dataframe


# --------------------- FUNCTIONS TO WRITE ENERGY DATA IN INFLUXDB --------------------- #


def chunk_and_write_dataframe(dataframe_to_write: pd.DataFrame, measurement: str,
                              user_id: str, df_client, batch_size: int = 5000) -> bool:
    """
    :param dataframe_to_write:
    :param measurement:
    :param user_id:
    :return:
    """
    # Chunk dataframe for time series db performance issues
    chunk_nb = math.ceil(len(dataframe_to_write) / batch_size)

    dataframe_chunk_list = np.array_split(dataframe_to_write, chunk_nb)
    # Write each chunk in time series db
    for chunk in dataframe_chunk_list:
        tags = {USER_PARAM_NAME: user_id}
        df_client.write_points(chunk, measurement=measurement, tags=tags, protocol="json")
    return True


def create_and_write_energy_for_user(user_id, client, df_client, accelerometer_measurement_name,
                                     five_sec_threshold, one_min_threshold, max_successive_time_diff,
                                     batch_size=5000):
    print("-----------------------")
    print("[Creation of features] user {}".format(user_id))

    # # 1. Compute global time interval
    first_timestamp_to_compute_energy = get_first_timestamp_to_compute_energy(user_id, client=client)
    day_range_to_query = get_time_difference_between_now_and_timestamp(first_timestamp_to_compute_energy)

    for day in reversed(range(day_range_to_query + 1)):
        # Extract raw data from InfluxDB for D-day
        start, end = str(day + 1) + "d", str(day) + "d"
        extracted_result_set = extract_raw_data_from_influxdb(client, accelerometer_measurement_name,
                                                              user_id, start, end)

        # Transform InfluxDB ResultSet in pandas Dataframe if resultset is not empty
        if extracted_result_set:
            tags = {"user": user_id}
            raw_acm_dataframe = transform_acm_result_set_into_dataframe(extracted_result_set, tags)
            print("Raw dataframe shape: {}".format(raw_acm_dataframe.shape))
        else:
            continue

        # 4. Compute the energy feature
        five_sec_energy_dataframe = create_energy_dataframe(raw_acm_dataframe,
                                                            aggregation_count_threshold=five_sec_threshold,
                                                            max_successive_time_diff=max_successive_time_diff,
                                                            aggregation_time="5s")
        if not five_sec_energy_dataframe.empty:
            # 5. Chunk resulting energy dataframe (if necessary) and write in influxdb
            chunk_and_write_dataframe(five_sec_energy_dataframe, accelerometer_measurement_name, user_id,
                                      df_client, batch_size=batch_size)

        # 4-bis. Compute the energy feature
        one_minute_energy_dataframe = create_energy_dataframe(raw_acm_dataframe,
                                                              aggregation_count_threshold=one_min_threshold,
                                                              max_successive_time_diff=max_successive_time_diff,
                                                              aggregation_time="1min")
        if not one_minute_energy_dataframe.empty:
            # 5-bis. Chunk resulting energy dataframe (if necessary) and write in influxdb
            chunk_and_write_dataframe(one_minute_energy_dataframe, accelerometer_measurement_name, user_id,
                                      df_client, batch_size=batch_size)

        print("[Written process done]")


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

    # MotionAccelerometer useful
    motion_acm_constants = config["Motion Accelerometer"]
    FIVE_SEC_THRESHOLD = motion_acm_constants["five_sec_threshold"]
    ONE_MIN_THRESHOLD = motion_acm_constants["one_min_threshold"]
    MAX_SUCCESSIVE_TIME_DIFF = motion_acm_constants["max_successive_time_diff"]

    # see InfluxDB Python API for more information
    # https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
    CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD, database=DB_NAME)
    DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD, database=DB_NAME)
    print("[Client created]")

    user_list = get_user_list(CLIENT)

    for user_id in user_list:
        create_and_write_energy_for_user(user_id, CLIENT, DF_CLIENT, ACCELEROMETER_MEASUREMENT_NAME,
                                         FIVE_SEC_THRESHOLD, ONE_MIN_THRESHOLD, MAX_SUCCESSIVE_TIME_DIFF,
                                         batch_size=5000)