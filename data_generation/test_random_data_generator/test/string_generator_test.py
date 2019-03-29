import source.random_data_generator_2 as rdg
import string


# Test if the string is empty if the size of the desired string equals 0
def test_string_generator_with_string_size_equals_zero():
    # GIVEN
    size_test = 0
    sequence_test = string.ascii_lowercase + string.digits

    # WHEN
    element = rdg.string_generator(size_test, sequence_test)

    # THEN
    assert element == ''


# Test if the length of the string equals the size of the desired string
def test_string_generator_with_different_size():
    # GIVEN
    size_test = [0, 3, 4]
    sequence_test = string.ascii_lowercase + string.digits

    for i in size_test:
        # WHEN
        element = rdg.string_generator(i, sequence_test)

        # THEN
        assert len(element) == i


