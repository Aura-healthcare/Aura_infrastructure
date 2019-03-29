# coding: utf-8

import argparse
import configparser
import datetime
from datetime import date
import string
import random
import os
import shutil
import logging

# CREATE LOGGERS #
LOG_FORMAT = '%(asctime)s :: %(levelname)s :: %(message)s'

RR_INTERVAL = 'RrInterval'
MOTION_ACCELEROMETER = 'MotionAccelerometer'
MOTION_GYROSCOPE = 'MotionGyroscope'

TIMESTAMP_PROBABLE_INTERVAL_BY_MEASUREMENT_DICT = {
    RR_INTERVAL: [200, 400],
    MOTION_ACCELEROMETER: [10, 30],
    MOTION_GYROSCOPE: [10, 30]
}

RANDOM_STRING_PARAMETER = {
    RR_INTERVAL: [1, '{:.0f}', ''],
    MOTION_ACCELEROMETER: [3, '{:.8f}', '2G'],
    MOTION_GYROSCOPE: [3, '{:.6f}', '']
}


USER = {
    'SIZE': [8, 4, 4, 4, 12],
    'CHAR_SET': string.ascii_lowercase + string.digits,
    'SEPARATOR': '-'
}

DEVICE = {
    'SIZE': [2, 2, 2, 2, 2, 2],
    'CHAR_SET': string.ascii_uppercase + string.digits,
    'SEPARATOR': ':'
}


def string_generator(size: int, chars: str) -> str:
    """
    Function generating random strings

    :param size: Output string size
    :param chars: set of value from which the value is randomly chosen
    :return: random string
    """

    return ''.join(random.choice(chars) for _ in range(size))


def timestamp_generator(number_of_data: int, measurement_type: str, logs: bool, year=date.today().year, month=date.today().month,
                        day=date.today().day, delta=1) -> list:
    """
    Function generating a list of timestamp

    :param number_of_data: number of generated data
    :param measurement_type: type of measurement, can be either RR_INTERVAL, MOTION_ACCELEROMETER or MOTION_GYROSCOPE
    :param year: chosen year for date
    :param month: chosen month for date
    :param day: chosen day for date
    :param delta: random time computed in this interval
    :return: list of timestamp
    """

    # CREATE LOGGER FOR DEBUG ABOUT TIMESTAMP #
    logger_timestamp = logging.getLogger('timestamp_debug')
    if logs:
        logger_timestamp.setLevel(logging.DEBUG)
    else:
        logger_timestamp.setLevel(logging.INFO)

    stream_handler_timestamp = logging.StreamHandler()
    stream_handler_timestamp.setLevel(logging.DEBUG)
    stream_handler_timestamp.setFormatter(logging.Formatter(LOG_FORMAT))

    logger_timestamp.addHandler(stream_handler_timestamp)

    if number_of_data > 0:
        logger_timestamp.info('list of {} timestamp to create'.format(number_of_data))
        start_timestamp = (datetime.datetime(year, month, day) + random.random() * datetime.timedelta(days=delta))
        logger_timestamp.debug('timestamp number 1 : {}'.format(start_timestamp))

        timestamp_list = list()
        timestamp_list.append(start_timestamp)
        timestamp = start_timestamp

        timedelta_lower_bound = TIMESTAMP_PROBABLE_INTERVAL_BY_MEASUREMENT_DICT[measurement_type][0]
        timedelta_upper_bound = TIMESTAMP_PROBABLE_INTERVAL_BY_MEASUREMENT_DICT[measurement_type][1]

        for i in range(number_of_data - 1):
            timestamp = timestamp + datetime.timedelta(
                milliseconds=random.randint(timedelta_lower_bound, timedelta_upper_bound))
            timestamp_list.append(timestamp)
            logger_timestamp.debug('timestamp number {} : {}'.format(i+2, timestamp))

        timestamp_list = [i.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3] for i in timestamp_list]

        logger_timestamp.removeHandler(stream_handler_timestamp)

        return timestamp_list
    else:
        logger_timestamp.warning("No timestamp needed or erroneous number of data : {}".format(number_of_data))
        logger_timestamp.removeHandler(stream_handler_timestamp)

        return []


def persist_file_to_disk(user_id: str, timestamp: str, build_str: str, measure_type: str):
    """
    Function storing string in a JSON file

    :param user_id: user's id
    :param timestamp: generated timestamp for the user
    :param build_str: JSON string
    :param measure_type: chosen field type (RrInterval, MotionGyroscope, MotionAccelerometer)
    :return: JSON file's name. The main goal is to store the string in a JSON file.
    """

    json_file_name = user_id + '_' + measure_type + '_' + timestamp.replace(':', '').replace('.', '') + ".json"

    json_file_name = json_file_name.replace('"', '')

    with open(json_file_name, 'w') as outfile:
        outfile.write(build_str)

    shutil.move(json_file_name, files_processing_paths["generated_files_directory"] + json_file_name)

    logger_process.info("\nfile {} moved in directory {}\n".format(json_file_name, files_processing_paths["generated_files_directory"]))


def generate_random_string(string_type: dict) -> str:
    """
    Function generating a random string based on sub sequences whose size are stored in a list
    :param string_type: dictionary with following shape
        EXAMPLE = {
            'SIZE': [2, 2, 2, 2],
            'CHAR_SET': string.ascii_lowercase + string.digits,
            'SEPARATOR': '-'
        }

    :return: string with following shape "l5-23-kj-9m"
    """
    if string_type['SIZE'] != [] and not all(v == 0 for v in string_type['SIZE']):
        random_string = '"'

        for value in string_type['SIZE']:
            random_string = random_string + string_generator(size=value, chars=string_type['CHAR_SET']) + string_type['SEPARATOR']

        random_string = random_string[0:-1]
        random_string = random_string + '"'

        return random_string
    else:
        print("{} is empty or only has sub sequences with length 0".format(string_type['SIZE']))
        return '""'


def generate_random_data_point(measurement: str, timestamp: str, logs: bool) -> str:
    """
    Function generating a random data point function of measurement
    :param measurement: RrInterval, MotionGyroscope or MotionAccelerometer
    :param timestamp:
    :return: a random data point
    """

    # CREATE LOGGER FOR DEBUG ABOUT DATA #
    logger_data = logging.getLogger('data_debug')
    if logs:
        logger_data.setLevel(logging.DEBUG)
    else:
        logger_data.setLevel(logging.INFO)

    stream_handler_data = logging.StreamHandler()
    stream_handler_data.setLevel(logging.DEBUG)
    stream_handler_data.setFormatter(logging.Formatter(LOG_FORMAT))

    logger_data.addHandler(stream_handler_data)

    if measurement in (RR_INTERVAL, MOTION_ACCELEROMETER, MOTION_GYROSCOPE):

        data = '"'
        data = data + timestamp + " "
        for i in range(RANDOM_STRING_PARAMETER[measurement][0]):

            if measurement == RR_INTERVAL:
                random_value = random.randint(300, 2000)
            else:
                random_value = random.uniform(-2, 2)

            data = data + str(RANDOM_STRING_PARAMETER[measurement][1].format(random_value)) + " "

        if RANDOM_STRING_PARAMETER[measurement][2] != '':
            data = data + RANDOM_STRING_PARAMETER[measurement][2] + " "

        data = data[0:-1] + '"'
        logger_data.debug('{} created'.format(data))
        logger_data.removeHandler(stream_handler_data)
        return data
    else:
        raise ValueError("Unknown measurement name : ", measurement)


def build_signal_to_data_points_count_dict(nb_data_rr: str, nb_data_ma: str, nb_data_mg: str) -> dict:
    """
    Function converting lists of strings in lists of integer

    :param nb_data_rr: Number of data AND Number of file for RrInterval 8000
    :param nb_data_ma: Number of data AND Number of file for MotionAccelerometer 7000
    :param nb_data_mg: Number of data AND Number of file for MotionGyroscope 6000
    :return: dictionary with measurement as key, number of data as for_loop[key]
    """

    signal_to_data_points_count_dict = {
        RR_INTERVAL: nb_data_rr,
        MOTION_ACCELEROMETER: nb_data_ma,
        MOTION_GYROSCOPE: nb_data_mg
    }

    return signal_to_data_points_count_dict


def generate_data_files(requirement_dict: dict, logs: bool):
    """
    Function creating a directory containing JSON files

    :param requirement_dict: dictionary with measurement as key and amount of data per measurement as value
    :return: directory with JSON files containing data. Each file contains 5000 data
    """

    # Creates random user and device
    user_id = generate_random_string(USER)
    device_address = generate_random_string(DEVICE)

    logger_process.info('\n### start process for USER : {} and DEVICE {} ###\n'.format(user_id, device_address))

    for measurement in requirement_dict:
        if requirement_dict[measurement] > 0:
            logger_process.info('\t # measurement {} requires {} data'.format(measurement, requirement_dict[measurement]))
            # Creates a list of timestamps for the measurement
            starting_date = config["Data Generation Starting Date"]
            timestamp_list = timestamp_generator(requirement_dict[measurement], measurement, logs, int(starting_date["year"]),
                                                 int(starting_date["month"]), int(starting_date["day"]))

            # Variables created to split create a JSON file every 5000 data
            nb_data_per_file = config["Number Of Data Per Files"]
            quotient, rest = requirement_dict[measurement] // int(nb_data_per_file["nb_data"]), requirement_dict[
                measurement] % int(nb_data_per_file["nb_data"])

            logger_process.info('\t{} file(s) with {} data and 1 file with {} data'.format(quotient, nb_data_per_file["nb_data"],
                                                                                         rest))

            for i in range(quotient):
                build_str = '{"user":' + user_id + ',"type":"' + measurement + '","device_address":' + device_address + ',"data":['
                for j in range(int(nb_data_per_file["nb_data"])):
                    build_str = build_str + generate_random_data_point(measurement, timestamp_list[
                        i * int(nb_data_per_file["nb_data"]) + j], logs) + ','
                build_str = build_str[0:-1] + ']}'
                persist_file_to_disk(user_id, timestamp_list[i * int(nb_data_per_file["nb_data"])],
                                     build_str, measurement)

            # Creates a file containing the remaining data if exist
            if rest != 0:
                build_str = '{"user":' + user_id + ',"type":"' + measurement + '","device_address":' + device_address + ',"data":['
                for j in range(rest):
                    build_str = build_str + generate_random_data_point(measurement, timestamp_list[
                        quotient * int(nb_data_per_file["nb_data"]) + j], logs) + ','
                build_str = build_str[0:-1] + ']}'
                persist_file_to_disk(user_id, timestamp_list[quotient * int(nb_data_per_file["nb_data"])],
                                     build_str, measurement)
        else:
            print('No data required for measurement {}'.format(measurement))


if __name__ == "__main__":
    RR_INTERVAL_DATA_POINTS_COUNT = 'RrInterval_datapoints_count'
    MOTION_ACC_DATA_POINTS_COUNT = 'MotionAcc_datapoints_count'
    MOTION_GYR_DATA_POINTS_COUNT = 'MotionGyr_datapoints_count'

    ap = argparse.ArgumentParser()
    ap.add_argument('-rr', '--' + RR_INTERVAL_DATA_POINTS_COUNT, type=int, required=False, default="0",
                    help="number of data point(s) for RrInterval")
    ap.add_argument('-ma', '--' + MOTION_ACC_DATA_POINTS_COUNT, type=int, required=False, default="0",
                    help="number of data point(s) for MotionAccelerometer")
    ap.add_argument('-mg', '--' + MOTION_GYR_DATA_POINTS_COUNT, type=int, required=False, default="0",
                    help="number of data point(s) for MotionGyroscope")
    args = vars(ap.parse_args())

    config = configparser.ConfigParser()
    config.read('config.conf')

    files_processing_paths = config["Paths"]
    if os.path.exists(files_processing_paths["generated_files_directory"]):
        shutil.rmtree(files_processing_paths["generated_files_directory"])
        os.makedirs(files_processing_paths["generated_files_directory"])
    else:
        os.makedirs(files_processing_paths["generated_files_directory"])

    # CREATE LOGGER FOR INFO ABOUT PROCESSING #
    logger_process = logging.getLogger('process_info')
    logger_process.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)

    logger_process.addHandler(stream_handler)

    # PROCESS #
    data_points_requirement = build_signal_to_data_points_count_dict(args[RR_INTERVAL_DATA_POINTS_COUNT],
                                                                    args[MOTION_ACC_DATA_POINTS_COUNT],
                                                                    args[MOTION_GYR_DATA_POINTS_COUNT])

    generate_data_files(data_points_requirement, config.getboolean('Logs', 'bool'))
