import source.random_data_generator_2 as rdg


# Test if final element type is int
def test_type_build_signal_to_data_points_count_dict():
    # GIVEN
    signal_test = [0, 0, 0]

    # WHEN
    element = rdg.build_signal_to_data_points_count_dict(signal_test[0], signal_test[1], signal_test[2])

    # THEN
    for measurement in element:
        assert type(element[measurement]) is int


# Test if final value for each measurement is not None or is different from empty string
def test_build_signal_to_data_points_count_dict_with_zero_data():
    # GIVEN
    signal_test = [0, 0, 0]

    # WHEN
    element = rdg.build_signal_to_data_points_count_dict(signal_test[0], signal_test[1], signal_test[2])

    # THEN
    for measurement in element:
        assert element[measurement] != '' or element[measurement] is not None

