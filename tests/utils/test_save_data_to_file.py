import os
import tempfile
import pytest
from jko_api_utils.utils.save_data import save_data_to_file, determine_mode_and_encoding


def test_save_data_to_file_empty_string():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        dest = f.name
    data = ""
    mode, encoding = determine_mode_and_encoding(data, None)
    save_data_to_file(data, dest, mode, encoding)
    with open(dest, "r") as f:
        assert f.read() == ""


def test_save_data_to_file_empty_bytes():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        dest = f.name
    data = b""
    mode, encoding = determine_mode_and_encoding(data, None)
    save_data_to_file(data, dest, mode, encoding)
    with open(dest, "rb") as f:
        assert f.read() == b""


def test_save_data_to_file_append_mode():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        dest = f.name
    data = "foo"
    mode, encoding = determine_mode_and_encoding(data, None)
    save_data_to_file(data, dest, mode, encoding)
    save_data_to_file(data, dest, "a", encoding)
    with open(dest, "r") as f:
        assert f.read() == "foofoo"


def test_save_data_to_file_readonly_file():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        dest = f.name
    data = "foo"
    mode, encoding = determine_mode_and_encoding(data, None)
    os.chmod(dest, 0o444)
    with pytest.raises(PermissionError):
        save_data_to_file(data, dest, mode, encoding)


def test_save_data_to_file_directory():
    with tempfile.TemporaryDirectory() as tempdir:
        with pytest.raises(IsADirectoryError):
            save_data_to_file("foo", tempdir, "w", "utf-8")


def test_save_data_to_file_newline_chars():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        dest = f.name
    data = "foo\nbar\r\nbaz"
    mode, encoding = determine_mode_and_encoding(data, "text")
    save_data_to_file(data, dest, mode, encoding)
    with open(dest, "r") as f:
        assert f.read() == "foo\nbar\nbaz"


def test_save_data_to_file_binary_data():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        dest = f.name
    data = b"\x00\x01\x02\x03\xff"
    mode, encoding = determine_mode_and_encoding(data, None)
    save_data_to_file(data, dest, mode, encoding)
    with open(dest, "rb") as f:
        assert f.read() == data


