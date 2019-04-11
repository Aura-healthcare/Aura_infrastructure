#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 14 17:36:50 2019

@author: romain.girard
"""
import argparse
import configparser
import library as l
from influxdb import InfluxDBClient
from influxdb import DataFrameClient

if __name__ == "__main__":

    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--directory", required=True, help="path to the data directory")
    args = vars(ap.parse_args())

    config = configparser.ConfigParser()
    config.read('config.conf')

    files_processing_paths = config["Paths"]

    PATH_TO_READ_DIRECTORY = args["directory"] + "/"
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
    CLIENT.create_database(DB_NAME)

    # -------- Write pipeline -------- #
    for measurement in ["RrInterval", "MotionAccelerometer", "MotionGyroscope"]:
        l.execute_write_pipeline(measurement, PATH_TO_READ_DIRECTORY, PATH_FOR_WRITTEN_FILES, PATH_FOR_PROBLEMS_FILES, CLIENT
                                 , DF_CLIENT, True)
        print("-----------------------")
