import os
import tempfile
import pytest

from jko_api_utils.utils.save_data import save_data_decorator


# Define test functions to be decorated with the save_data_decorator

def return_str():
    return "Test string"


def return_bytes():
    return b"Test bytes"


def generate_str():
    for i in range(3):
        yield f"String {i}"


def generate_bytes():
    for i in range(3):
        yield bytes(f"Bytes {i}", encoding="utf-8")


# Define fixture functions

@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def tmp_file(tmp_dir):
    return os.path.join(tmp_dir, "test_file.txt")


# Define test functions for the decorator

def test_save_data_decorator_string(tmp_file):
    decorated_func = save_data_decorator(return_str)
    assert decorated_func(dest=tmp_file, return_data=True) == "Test string"
    assert os.path.isfile(tmp_file)


def test_save_data_decorator_bytes(tmp_file):
    decorated_func = save_data_decorator(return_bytes)
    assert decorated_func(dest=tmp_file, return_data=True) == b"Test bytes"
    assert os.path.isfile(tmp_file)


def test_save_data_decorator_generator_string(tmp_file):
    decorated_func = save_data_decorator(generate_str)
    assert decorated_func(dest=tmp_file, return_data=True) == [
        "String 0", "String 1", "String 2"]
    assert os.path.isfile(tmp_file)


def test_save_data_decorator_generator_bytes(tmp_file):
    decorated_func = save_data_decorator(generate_bytes)
    assert decorated_func(dest=tmp_file, return_data=True) == [
        b"Bytes 0", b"Bytes 1", b"Bytes 2"]
    assert os.path.isfile(tmp_file)


def test_save_data_decorator_missing_file(tmp_dir):
    decorated_func = save_data_decorator(return_str)
    with pytest.raises(ValueError):
        decorated_func(dest=None, return_data=False)


def test_save_data_decorator_missing_dir(tmp_file):
    decorated_func = save_data_decorator(return_str)
    with pytest.raises(ValueError):
        decorated_func(
            dest="/nonexistent/directory/test_file.txt", return_data=False)


def test_save_data_decorator_non_string_or_bytes(tmp_file):
    def return_int():
        return 123
    decorated_func = save_data_decorator(return_int)
    with pytest.raises(TypeError):
        decorated_func(dest=tmp_file, return_data=True)
