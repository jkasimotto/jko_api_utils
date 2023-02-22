import pytest
import os
from tempfile import TemporaryDirectory
from io import StringIO
from pathlib import Path
from enum import Enum, auto

from jko_api_utils.utils.save_data import save_to_file, DuplicateStrategy


@pytest.fixture(scope='module')
def temp_dir():
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

def test_save_to_file_with_list(temp_dir):
    data = ['apple', 'banana', 'cherry']
    paths = [temp_dir / 'fruit1.txt', temp_dir / 'fruit2.txt', temp_dir / 'fruit3.txt']
    result = save_to_file(data, path_list=paths)

    for path, expected in zip(paths, data):
        assert path.exists()
        assert path.read_text() == expected

    assert result == data

def test_save_to_file_with_generator(temp_dir):
    data = (fruit for fruit in ['apple', 'banana', 'cherry'])
    paths = [temp_dir / 'fruit1.txt', temp_dir / 'fruit2.txt', temp_dir / 'fruit3.txt']
    result = save_to_file(data, path_list=paths)

    for path, expected in zip(paths, ['apple', 'banana', 'cherry']):
        assert path.exists()
        assert path.read_text() == expected

    assert result == ['apple', 'banana', 'cherry']

def test_save_to_file_with_duplicate_strategy_rename(temp_dir):
    data = ['apple', 'banana', 'cherry']
    paths = [temp_dir / 'fruit1.txt', temp_dir / 'fruit2.txt', temp_dir / 'fruit3.txt']
    result = save_to_file(data, path_list=paths, duplicate_strategy=DuplicateStrategy.RENAME)

    expected_paths = [temp_dir / 'fruit1.txt', temp_dir / 'fruit2.txt', temp_dir / 'fruit3.txt']
    expected_data = ['apple', 'banana', 'cherry']

    # Create the file fruit2.txt
    paths[1].touch()

    for path, expected in zip(expected_paths, expected_data):
        assert path.exists()
        assert path.read_text() == expected

    assert result == expected_data

    # Clean up the file fruit2.txt
    paths[1].unlink()

def test_save_to_file_with_duplicate_strategy_overwrite(temp_dir):
    data = ['apple', 'banana', 'cherry', 'banana']
    paths = [temp_dir / 'fruit1.txt', temp_dir / 'fruit2.txt', temp_dir / 'fruit3.txt', temp_dir / 'fruit2.txt']
    result = save_to_file(data, path_list=paths, duplicate_strategy=DuplicateStrategy.OVERWRITE)

    expected_paths = [temp_dir / 'fruit1.txt', temp_dir / 'fruit2.txt', temp_dir / 'fruit3.txt', temp_dir / 'fruit2.txt']
    expected_data = ['apple', 'banana', 'cherry', 'banana']

    for path, expected in zip(expected_paths, expected_data):
        assert path.exists()
        assert path.read_text() == expected

    assert result == expected_data
