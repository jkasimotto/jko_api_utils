import os
from tempfile import TemporaryDirectory

import pytest

from jko_api_utils.utils.save_data import create_dirs_if_needed


def test_create_single_missing_directory():
    with TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "dir1", "file.txt")
        create_dirs_if_needed(path, create_missing_dirs=True)
        assert os.path.isdir(os.path.dirname(path))


def test_create_multiple_missing_directories():
    with TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "dir1", "dir2", "dir3")
        create_dirs_if_needed(path, create_missing_dirs=True)
        assert os.path.isdir(path)


def test_does_not_create_existing_directories():
    with TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "dir1", "dir2", "dir3")
        os.makedirs(path)
        create_dirs_if_needed(path, create_missing_dirs=True)
        assert os.path.isdir(path)


def test_raises_value_error_if_create_missing_dirs_is_false():
    with TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "dir1", "dir2", "dir3")
        with pytest.raises(ValueError):
            create_dirs_if_needed(path, create_missing_dirs=False)


def test_creates_directories_with_correct_permissions():
    with TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "dir1", "dir2", "dir3")
        create_dirs_if_needed(path, create_missing_dirs=True)
        # check that the permissions on the created directories are correct
        assert oct(os.stat(path).st_mode)[-3:] == "755"
        assert oct(os.stat(os.path.dirname(path)).st_mode)[-3:] == "755"
