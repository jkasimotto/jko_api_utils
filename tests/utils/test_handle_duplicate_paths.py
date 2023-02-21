import os
import pytest
from pathlib import Path
from jko_api_utils.utils.save_data import preprocess_duplicate_paths, DuplicateStrategy

def test_preprocess_duplicate_paths_overwrite():
    path_list = [
        "file1.txt",
        "file2.txt",
        "file3.txt"
    ]
    duplicate_strategy = DuplicateStrategy.OVERWRITE

    # Create a file with the same name as file1.txt
    with open("file1.txt", "w") as f:
        f.write("test")

    # Run the function
    new_path_list = preprocess_duplicate_paths(path_list, duplicate_strategy)

    # Check that the function returns a new list with the correct paths
    assert new_path_list == [
        "file1.txt",
        "file2.txt",
        "file3.txt"
    ]

    # Clean up
    os.remove("file1.txt")

def test_preprocess_duplicate_paths_rename():
    path_list = [
        "file1.txt",
        "file2.txt",
        "file3.txt"
    ]
    duplicate_strategy = DuplicateStrategy.RENAME

    # Create a file with the same name as file1.txt
    with open("file1.txt", "w") as f:
        f.write("test")

    # Run the function
    new_path_list = preprocess_duplicate_paths(path_list, duplicate_strategy)

    # Check that the function returns a new list with the correct paths
    assert new_path_list == [
        "file1_1.txt",
        "file2.txt",
        "file3.txt"
    ]

    # Clean up
    os.remove("file1.txt")

def test_preprocess_duplicate_paths_skip():
    path_list = [
        "file1.txt",
        "file2.txt",
        "file3.txt"
    ]
    duplicate_strategy = DuplicateStrategy.SKIP

    # Create a file with the same name as file1.txt
    with open("file1.txt", "w") as f:
        f.write("test")

    # Run the function
    new_path_list = preprocess_duplicate_paths(path_list, duplicate_strategy)

    # Check that the function returns a new list with the correct paths
    assert new_path_list == [
        "file2.txt",
        "file3.txt"
    ]

    # Clean up
    os.remove("file1.txt")
