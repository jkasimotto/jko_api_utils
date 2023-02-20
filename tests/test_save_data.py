import os
import tempfile
import pytest
from jko_api_utils.utils import save_data


def test_save_data_text(tmpdir):
    # Test saving text data to a file
    data = "hello, world!"
    dest_file = os.path.join(tmpdir, "test.txt")
    save_data(data, dest_file=dest_file)
    with open(dest_file) as f:
        assert f.read() == data


def test_save_data_binary(tmpdir):
    # Test saving binary data to a file
    data = b"\x00\x01\x02\x03"
    dest_file = os.path.join(tmpdir, "test.bin")
    save_data(data, dest_file=dest_file)
    with open(dest_file, "rb") as f:
        assert f.read() == data


def test_save_data_return_text():
    # Test returning text data
    data = "hello, world!"
    result = save_data(data)
    assert result == data


def test_save_data_return_binary():
    # Test returning binary data
    data = b"\x00\x01\x02\x03"
    result = save_data(data)
    assert result == data.decode("utf-8")


def test_save_data_error():
    # Test error when dest_file is None and return_data is False
    with pytest.raises(ValueError):
        save_data("hello, world!", dest_file=None, return_data=False)


def test_save_data_format_text(tmpdir):
    # Test saving text data with specified format
    data = "hello, world!"
    dest_file = os.path.join(tmpdir, "test.txt")
    save_data(data, dest_file=dest_file, data_format="text")
    with open(dest_file) as f:
        assert f.read() == data


def test_save_data_format_binary(tmpdir):
    # Test saving binary data with specified format
    data = b"\x00\x01\x02\x03"
    dest_file = os.path.join(tmpdir, "test.bin")
    save_data(data, dest_file=dest_file, data_format="binary")
    with open(dest_file, "rb") as f:
        assert f.read() == data
    

def test_save_data_invalid_dest():
    # Test error when destination path is invalid and create_dirs is False
    with pytest.raises(ValueError):
        save_data("hello, world!", dest_file="/invalid/path/test.txt", create_dirs=False)