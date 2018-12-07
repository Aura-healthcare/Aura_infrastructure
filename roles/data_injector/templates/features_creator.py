#!/usr/bin/env python
# coding: utf-8
"""TODO"""

import os
import configparser
from typing import List
import pandas as pd
import numpy as np
from influxdb import InfluxDBClient
from influxdb import DataFrameClient

# JSON field values
TYPE_PARAM_NAME = "type"
USER_PARAM_NAME = "user"
DEVICE_PARAM_NAME = "device_address"


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
    result = client.query(influx_query)

    usr_list = []
    points = list(result.get_points(measurement=measurment))
    for elt in points:
        usr_list.append(elt["value"])

    return usr_list


def transform_acm_result_set_into_dataframe(result_set, measurement: str, tags: dict) -> pd.DataFrame:
    """
    TODO

    :param result_set:
    :param measurement:
    :param tags:
    :return:
    """
    raw_acm_data_list = list(result_set.get_points(measurement=measurement, tags=tags))
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
    mask = acm_dataframe["time"].diff(periods=1) < "00:00:00.1"
    diff = acm_dataframe[mask].diff(periods=1).drop(columns=["time"])
    squared_df = diff ** 2
    sum_df = squared_df.sum(axis=1).to_frame()
    sum_df = sum_df.rename(columns={0: "squared_diff_sum"})
    sum_df.index.name = "timestamp"
    return sum_df


def write_energy_points_for_user(user: str) -> bool:
    """
    TODO

    :param user:
    :return:
    """
    print("[Creation of features] user {}".format(user))

    for day in range(120, 160):
        # Extract raw data from InfluxDB (for N last days)
        query = "SELECT * FROM {} WHERE \"user\" = '".format(MEASUREMENT) + user + "' and time < now() - {}d and time > now() - {}d".format(day, day+1)
        result_query = CLIENT.query(query)

        # Transform data in a pandas Dataframe
        tags = {"user": user}
        try:
            raw_acm_dataframe = transform_acm_result_set_into_dataframe(result_query, MEASUREMENT, tags)
        except KeyError:
            continue
        # Create energy features data points
        sum_df = create_energy_data_points(raw_acm_dataframe)
        print(sum_df.shape)
        row_nb = len(sum_df)
        if row_nb >= 1000000:
            for chunk in np.array_split(sum_df, 200):
                # Write new features into influxdb
                tags = {USER_PARAM_NAME: user}
                DF_CLIENT.write_points(chunk, measurement=MEASUREMENT, tags=tags, protocol="json")
        elif 1000000 > row_nb > 100000:
            for chunk in np.array_split(sum_df, 20):
                # Write new features into influxdb
                tags = {USER_PARAM_NAME: user}
                DF_CLIENT.write_points(chunk, measurement=MEASUREMENT, tags=tags, protocol="json")
        else:
            # Write new features into influxdb
            tags = {USER_PARAM_NAME: user}
            DF_CLIENT.write_points(sum_df, measurement=MEASUREMENT, tags=tags, protocol="json")

    print("[Writing process Done]")
    return True


if __name__ == "__main__":

    run_path = os.path.dirname(os.path.abspath(__file__))

    config = configparser.ConfigParser()
    config.read(run_path + '/config.conf')

    influxdb_client_constants = config["Influxdb Client"]
    DB_NAME = influxdb_client_constants["database_name"]
    HOST = influxdb_client_constants["host"]
    PORT = int(influxdb_client_constants["port"])
    USER = influxdb_client_constants["user"]
    PASSWORD = influxdb_client_constants["password"]

    MEASUREMENT = "MotionAccelerometer"
    # see InfluxDB Python API for more information
    # https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
    CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)

    # Create influxDB dataframe client
    DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                                database=DB_NAME)
    print("[Client created]")

    # Get list of all users
    user_list = get_user_list(CLIENT, MEASUREMENT)
    # user_list = ["2b3d7a98-a1fb-47ad-9705-4a91d448da9b"]

    for user in user_list:
        write_energy_points_for_user(user)
