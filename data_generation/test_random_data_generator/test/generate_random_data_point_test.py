import source.random_data_generator_2 as rdg

RR_INTERVAL = 'RrInterval'
MOTION_ACCELEROMETER = 'MotionAccelerometer'
MOTION_GYROSCOPE = 'MotionGyroscope'


def test_generate_random_data_point_with_random_timestamp_for_rr_measurement():
    # GIVEN
    measurement = RR_INTERVAL
    timestamp = '2019-03-08T15:40:36.188'
    logs = False

    # WHEN
    element = rdg.generate_random_data_point(measurement, timestamp, logs)

    # THEN
    assert len(element.replace('"', '').split()) == 2


def test_generate_random_data_point_with_random_timestamp_for_mg_measurement():
    # GIVEN
    measurement = MOTION_GYROSCOPE
    timestamp = '2019-03-08T15:40:36.188'
    logs = False

    # WHEN
    element = rdg.generate_random_data_point(measurement, timestamp, logs)

    # THEN
    assert len(element.replace('"', '').split()) == 4


def test_generate_random_data_point_with_random_timestamp_for_ma_measurement():
    # GIVEN
    measurement = MOTION_ACCELEROMETER
    timestamp = '2019-03-08T15:40:36.188'
    logs = False

    # WHEN
    element = rdg.generate_random_data_point(measurement, timestamp, logs)

    # THEN
    assert len(element.replace('"', '').split()) == 5


def test_generate_random_data_point_with_no_timestamp_for_rr_measurement():
    # GIVEN
    measurement = RR_INTERVAL
    timestamp = ''
    logs = False

    # WHEN
    element = rdg.generate_random_data_point(measurement, timestamp, logs)

    # THEN
    assert len(element.replace('"', '').split()) == 1


def test_generate_random_data_point_with_no_timestamp_for_mg_measurement():
    # GIVEN
    measurement = MOTION_GYROSCOPE
    timestamp = ''
    logs = False

    # WHEN
    element = rdg.generate_random_data_point(measurement, timestamp, logs)

    # THEN
    assert len(element.replace('"', '').split()) == 3


def test_generate_random_data_point_with_no_timestamp_for_ma_measurement():
    # GIVEN
    measurement = MOTION_ACCELEROMETER
    timestamp = ''
    logs = False

    # WHEN
    element = rdg.generate_random_data_point(measurement, timestamp, logs)

    # THEN
    assert len(element.replace('"', '').split()) == 4
