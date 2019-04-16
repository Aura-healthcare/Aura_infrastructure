import source.random_data_generator_2 as rdg

RR_INTERVAL = 'RrInterval'
MOTION_ACCELEROMETER = 'MotionAccelerometer'
MOTION_GYROSCOPE = 'MotionGyroscope'


# Test list content if number of data equals 0
def test_timestamp_generator_with_zero_data_point_rr_for_measurement():
    # GIVEN
    nb_of_points_test = 0
    measurement_test = RR_INTERVAL
    logs = False

    # WHEN
    element = rdg.timestamp_generator(nb_of_points_test, measurement_test, logs)

    # THEN
    assert element == []


# Test if the length of the list matches the number of elements
def test_timestamp_generator_content_with_zero_data_point_rr_for_measurement():
    # GIVEN
    nb_of_points_test = 0
    measurement_test = RR_INTERVAL
    logs = False

    # WHEN
    element = rdg.timestamp_generator(nb_of_points_test, measurement_test, logs)

    # THEN
    assert len(element) == 0


# Test if the length of the list matches the number of elements
def test_timestamp_generator_with_ten_data_points_rr_for_measurement():
    # GIVEN
    nb_of_points_test = 10
    measurement_test = RR_INTERVAL
    logs = False

    # WHEN
    element = rdg.timestamp_generator(nb_of_points_test, measurement_test, logs)

    # THEN
    assert len(element) == 10


# Test if the length of the list matches the number of elements
def test_timestamp_generator_with_hundred_data_points_rr_for_measurement():
    # GIVEN
    nb_of_points_test = 100
    measurement_test = RR_INTERVAL
    logs = False

    # WHEN
    element = rdg.timestamp_generator(nb_of_points_test, measurement_test, logs)

    # THEN
    assert len(element) == 100


# Test if the length of the list matches the number of elements
def test_timestamp_generator_with_thousand_data_points_rr_for_measurement():
    # GIVEN
    nb_of_points_test = 1000
    measurement_test = RR_INTERVAL
    logs = False

    # WHEN
    element = rdg.timestamp_generator(nb_of_points_test, measurement_test, logs)

    # THEN
    assert len(element) == 1000


# Test if the elements are different one another
def test_timestamp_generator_different_data_points_rr_measurement():
    # GIVEN
    nb_of_points_test = 10
    measurement_test = RR_INTERVAL
    logs = False

    # WHEN
    element = rdg.timestamp_generator(nb_of_points_test, measurement_test, logs)

    # THEN
    for i in range(len(element)-1):
        assert element[i] != element[i+1]
