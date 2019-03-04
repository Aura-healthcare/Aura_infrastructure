# coding: utf-8

import argparse
import configparser
import datetime
from datetime import date
import string
import random
import os
import shutil


def string_generator(size, chars=string.ascii_uppercase + string.digits):
    """
    Function generating random strings

    :param size: Output string size
    :param chars: set of value from which the value is randomly chosen
    :return: random string
    """

    return ''.join(random.choice(chars) for _ in range(size))


def generate_random_user():
    """
    Function generating a random user

    :return: random user id
    """

    user_id = '"'
    size_list = [8, 4, 4, 4, 12]

    for value in size_list:
        user_id = user_id + string_generator(size=value, chars=string.ascii_lowercase + string.digits) + "-"

    user_id = user_id[0:-1]
    user_id = user_id + '"'

    return user_id


def generate_random_device_address():
    """
    Function generating a random device address

    :return: random device address
    """

    device_address = '"'
    size_list = [2, 2, 2, 2, 2, 2]

    for value in size_list:
        device_address = device_address + string_generator(size=value, chars=string.ascii_uppercase + string.digits) + ":"

    device_address = device_address[0:-1]
    device_address = device_address + '"'

    return device_address


def timestamp_generator(number_of_data, year=date.today().year, month=date.today().month, day=date.today().day, delta=1):
    """
    Function generating a list of timestamp

    :param number_of_data: number of generated data
    :param year: chosen year for date
    :param month: chosen month for date
    :param day: chosen day for date
    :param delta: random time computed in this interval
    :return: list of timestamp
    """

    timestamp = (datetime.datetime(year, month, day) + random.random() * datetime.timedelta(days=delta))

    timestamp_list = list()
    timestamp_list.append(timestamp)

    for i in range(number_of_data-1):
        timestamp = timestamp + datetime.timedelta(milliseconds=random.randint(1, 3))
        timestamp_list.append(timestamp)

    timestamp_list = [i.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] for i in timestamp_list]

    return timestamp_list


def generate_random_motion_accelerometer_data(timestamp):
    """
    Function generating random motion accelerometer data

    :param timestamp: generated timestamp for the user
    :return: random motion accelerometer data : "2018-10-04T19:17:45.753 -0.2218628 0.88500977 -0.46984863 2G"
    """

    data = '"'
    data = data + timestamp + " " + str("{:.8f}".format(random.uniform(-20, 20))) \
           + " " + str("{:.8f}".format(random.uniform(-20, 20))) + " " + str("{:.8f}".format(random.uniform(-20, 20))) + ' 2G'
    data = data + '"'

    return data


def generate_random_motion_gyroscope_data(timestamp):
    """
    Function generating random motion gyroscope data

    :param timestamp: generated timestamp for the user
    :return: random motion gyroscope data : "2018-10-04T19:24:36.600 1.402439 -21.280489 -10.152439"
    """

    data = '"'
    data = data + timestamp + ' ' + str('{:.6f}'.format(random.uniform(-20, 20))) \
           + ' ' + str('{:.6f}'.format(random.uniform(-20, 20))) + ' ' + str('{:.6f}'.format(random.uniform(-20, 20)))
    data = data + '"'

    return data


def generate_random_rr_interval_data(timestamp):
    """
    Function generating random rr interval data

    :param timestamp: generated timestamp for the user
    :return: random Rr interval data : "2018-10-04T16:33:33.872 522"
    """

    data = '"'
    data = data + timestamp + ' ' + str(random.randint(300, 2000))
    data = data + '"'

    return data


def convert_args_to_int(nb_data_rr=0, nb_data_ma=0, nb_data_mg=0):
    """
    Function converting lists of strings in lists of integer

    :param nb_data_rr: Number of data AND Number of file for RrInterval 8000
    :param nb_data_ma: Number of data AND Number of file for MotionAccelerometer 7000
    :param nb_data_mg: Number of data AND Number of file for MotionGyroscope 6000
    :return: dictionary with measurement as key, number of data as for_loop[key]
    """

    for_loop = {
        "RrInterval": int(nb_data_rr),
        "MotionAccelerometer": int(nb_data_ma),
        "MotionGyroscope": int(nb_data_mg)
    }

    return for_loop


def string_to_json_file(user_id, timestamp, build_str, measure_type):
    """
    Function storing string in a JSON file

    :param user_id: user's id
    :param timestamp: generated timestamp for the user
    :param build_str: JSON string
    :param measure_type: chosen field type (RrInterval, MotionGyroscope, MotionAccelerometer)
    :return: JSON file's name. The main goal is to store the string in a JSON file.
    """

    json_file_name = user_id + '_' + measure_type + '_' + str(timestamp).replace(':', '').replace('.', '') + ".json"

    json_file_name = json_file_name.replace('"', '')

    with open(json_file_name, 'w') as outfile:
        outfile.write(build_str)

    return json_file_name


def build_string_for_json_file(measurement, timestamp):
    """
    Function generating the right random data according to the measurement

    :param measurement: type of data
    :param timestamp: timestamp corresponding to the data
    :return: random generated data
    """

    if measurement == "RrInterval":
        build_str = generate_random_rr_interval_data(timestamp) + ','
    elif measurement == "MotionGyroscope":
        build_str = generate_random_motion_gyroscope_data(timestamp) + ','
    else:
        build_str = generate_random_motion_accelerometer_data(timestamp) + ','

    return build_str


def execute_pipeline(args_measurement_nb_data):
    """
    Function creating a directory containing JSON files

    :param args_measurement_nb_data: dictionary with measurement as key and amount of data per measurement as value
    :return: directory with JSON files containing data. Each file contains 5000 data
    """
    # Creates random user and device
    user_id = generate_random_user()
    device_address = generate_random_device_address()

    for key in args_measurement_nb_data:
        # Creates a list of timestamps for the measurement
        starting_date = config["Data generation starting date"]
        timestamp_list = timestamp_generator(args_measurement_nb_data[key], int(starting_date["year"]), int(starting_date["month"]), int(starting_date["day"]))

        # Variables created to split create a JSON file every 5000 data
        nb_data_per_file = config["Number of data per files"]
        quotient, rest = args_measurement_nb_data[key]//int(nb_data_per_file["nb_data"]), args_measurement_nb_data[key] % int(nb_data_per_file["nb_data"])

        # Creates as much "limit" data files as possible
        for i in range(quotient):
            build_str = '{"user":' + user_id + ',"type":"' + key + '","device_address":' + device_address + ',"data":['
            for j in range(int(nb_data_per_file["nb_data"])):
                build_str = build_str + build_string_for_json_file(key, timestamp_list[i*int(nb_data_per_file["nb_data"]) + j])
            build_str = build_str[0:-1] + ']}'
            json_file_name = string_to_json_file(user_id, timestamp_list[i*int(nb_data_per_file["nb_data"])], build_str, key)
            shutil.move(json_file_name, files_processing_paths["generated_files_directory"] + json_file_name)

        # Creates a file containing the remaining data if exist
        if rest != 0:
            build_str = '{"user":' + user_id + ',"type":"' + key + '","device_address":' + device_address + ',"data":['
            for j in range(rest):
                build_str = build_str + build_string_for_json_file(key, timestamp_list[quotient*int(nb_data_per_file["nb_data"]) + j])
            build_str = build_str[0:-1] + ']}'
            json_file_name = string_to_json_file(user_id, timestamp_list[quotient * int(nb_data_per_file["nb_data"])], build_str, key)
            shutil.move(json_file_name, files_processing_paths["generated_files_directory"] + json_file_name)

    return build_str


if __name__ == "__main__":

    ap = argparse.ArgumentParser()
    ap.add_argument('-nbr', '--data_RrInterval', required=False, default="0", help="number of data for RrInterval")
    ap.add_argument('-nbma', '--data_MotionAccelerometer', required=False, default="0", help="number of data for MotionAccelerometer")
    ap.add_argument('-nbmg', '--data_MotionGyroscope', required=False, default="0",help="number of data for MotionGyroscope")
    args = vars(ap.parse_args())

    config = configparser.ConfigParser()
    config.read('config.conf')

    files_processing_paths = config["Paths"]
    if os.path.exists(files_processing_paths["generated_files_directory"]):
        shutil.rmtree(files_processing_paths["generated_files_directory"])
        os.makedirs(files_processing_paths["generated_files_directory"])
    else:
        os.makedirs(files_processing_paths["generated_files_directory"])

    args = convert_args_to_int(args["data_RrInterval"], args["data_MotionAccelerometer"], args["data_MotionGyroscope"])

    execute_pipeline(args)
