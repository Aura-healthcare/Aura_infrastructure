import source.random_data_generator_2 as rdg
import string


# Test if the list is empty
def test_empty_list():
    # GIVEN
    empty_test_list = {
        'SIZE': [],
        'CHAR_SET': string.ascii_lowercase + string.digits,
        'SEPARATOR': '-'
    }

    # WHEN
    element = rdg.generate_random_string(empty_test_list)

    # THEN
    assert element == '""'


# Test the length of the string if the size list is only composed of 1-length substrings
def test_unit_list():
    # GIVEN
    unit_test_list = {
        'SIZE': [1, 1, 1, 1, 1],
        'CHAR_SET': string.ascii_lowercase + string.digits,
        'SEPARATOR': '-'
    }

    # WHEN
    element = rdg.generate_random_string(unit_test_list)

    # THEN
    assert len(element) == 11

