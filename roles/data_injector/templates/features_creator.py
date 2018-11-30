#!/usr/bin/env python
# coding: utf-8
"""TODO"""

import configparser
from typing import List
import math
import pandas as pd
import numpy as np
from influxdb import InfluxDBClient
from influxdb import DataFrameClient

# JSON field values
TYPE_PARAM_NAME = "type"
USER_PARAM_NAME = "user"
DEVICE_PARAM_NAME = "device_address"

ACCELEROMETER_MEASUREMENT_NAME = "MotionAccelerometer"


def get_user_list(client, measurment: str = "RrInterval") -> List[str]:
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
    user_values_dict = list(query_result.get_points(measurement=measurment))

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
    return raw_acm_dataframe


def create_energy_data_points(acm_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    TODO

    :param acm_dataframe:
    :return:
    """
    boolean_mask = acm_dataframe["time"].diff(periods=1) < "00:00:00.1"
    consecutive_differences_dataframe = acm_dataframe.diff(periods=1)[boolean_mask].drop(["time"], axis=1)
    squared_differences_dataframe = consecutive_differences_dataframe ** 2

    triaxial_sum_dataframe = squared_differences_dataframe.sum(axis=1).to_frame()
    triaxial_sum_dataframe = triaxial_sum_dataframe.rename(columns={0: "squared_diff_sum"})

    acm_dataframe_index = acm_dataframe[boolean_mask]["time"]
    triaxial_sum_dataframe.index = acm_dataframe_index
    triaxial_sum_dataframe.index.name = "timestamp"
    return triaxial_sum_dataframe


def write_energy_points_for_user(user: str, first_day: int, last_day: int) -> bool:
    """
    TODO
    :param user:
    :param first_day:
    :param last_day:
    :return:
    """
    print("[Creation of features] user {}".format(user))

    for day in range(first_day, last_day):
        # Extract raw data from InfluxDB for D-day
        query = "SELECT * FROM {} WHERE \"user\" = '{}' and time < now() - {}d and time > now() - {}d".format(MEASUREMENT, user, day, day+1)
        extracted_data_result_set = CLIENT.query(query)

        # Transform InfluxDB ResultSet in pandas Dataframe
        tags = {"user": user}
        try:
            raw_acm_dataframe = transform_acm_result_set_into_dataframe(extracted_data_result_set, tags)
        except KeyError:
            # If there is no data for D-day, skip processing
            continue

        # Create triaxial sum data points
        triaxial_sum_dataframe = create_energy_data_points(raw_acm_dataframe)

        # Write new features into influxdb
        # Chunk dataframe for influxdb performance issues
        chunk_nb = math.ceil(len(triaxial_sum_dataframe) / 5000)
        for chunk in np.array_split(triaxial_sum_dataframe, chunk_nb):
            print("chunk shape : {}".format(chunk.shape))
            tags = {USER_PARAM_NAME: user}
            DF_CLIENT.write_points(chunk, measurement=MEASUREMENT, tags=tags, protocol="json")

    print("[Writing process Done]")
    return True


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
    CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)
    DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                                database=DB_NAME)
    print("[Client created]")

    MEASUREMENT = "MotionAccelerometer"
    user_list = get_user_list(CLIENT, MEASUREMENT)

    for user in user_list:
        write_energy_points_for_user(user=user, first_day=120, last_day=160)
