import pytest
from jko_api_utils.utils.save_data import convert_data_to_string

def test_convert_data_to_string_with_string():
    data = "hello world"
    assert convert_data_to_string(data, "text") == data
    assert convert_data_to_string(data, None) == data

def test_convert_data_to_string_with_bytes():
    data = b"hello world"
    assert convert_data_to_string(data, "binary") == "hello world"
    assert convert_data_to_string(data, None) == "hello world"

def test_convert_data_to_string_with_invalid_data():
    data = 123
    with pytest.raises(ValueError):
        convert_data_to_string(data, "text")

    with pytest.raises(ValueError):
        convert_data_to_string(data, "binary")
