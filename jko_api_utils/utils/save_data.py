from enum import Enum
import os
from functools import wraps
from typing import Optional
from pathlib import Path


class DuplicateStrategy(Enum):
    OVERWRITE = "overwrite"
    RENAME = "rename"
    SKIP = "skip"


def create_dirs_if_needed(path, create_missing_dirs=False):
    """
    Create directories for the given  path if they do not already exist.

    If the parent directory of the  path does not exist and `create_dirs` is False, this
    function raises a ValueError. If `create_dirs` is True, this function creates the directory
    and any intermediate directories as needed.

    :param path: The  path to create directories for.
    :param create_missing_dirs: Whether to create the directories if they do not already exist.
    :raises ValueError: If the parent directory of the  path does not exist and
        `create_dirs` is False.
    """
    if os.path.dirname(path) and not os.path.exists(os.path.dirname(path)):
        if not create_missing_dirs:
            raise ValueError(
                f"Destination directory does not exist: {os.path.dirname(path)}")
        os.makedirs(path, exist_ok=True)


def determine_mode_and_encoding(data, append=False):
    """
    Determines the appropriate mode and encoding for writing the given data to a file based on its format.

    :param data: The data to be written to the file.
    # TODO: Enum, json, yaml
    :param append: Whether to append to the file or overwrite it.
    :return: A tuple of the mode and encoding to use for writing the data to a file.
    :raises ValueError: If the data format is invalid.
    """
    if append:
        mode = "a" if isinstance(data, str) else "ab"
        encoding = "utf-8" if isinstance(data, str) else None
    else:
        mode = "w" if isinstance(data, str) else "wb"
        encoding = "utf-8" if isinstance(data, str) else None
        if mode not in ["w", "wb", "a", "ab"]:
            raise ValueError("Invalid mode")

    if encoding not in [None, "utf-8"]:
        raise ValueError("Invalid encoding")

    return mode, encoding


def save_data_to_file(data, dest, mode, encoding):
    """
    Writes the given data to the specified file.

    :param data: The data to be written.
    :param dest: The path to the file.
    :param mode: The mode in which the file should be opened. Must be "w" for text files or "wb" for binary files, "a" for appending or "ab" for appending binary.
    :param encoding: The encoding to be used when writing text data. For binary data, this should be None.
    :raises ValueError: If the mode parameter is not "w" or "wb".
    :raises TypeError: If the encoding parameter is not a string.
    :raises IOError: If an error occurs while writing to the file.
    """
    if mode not in ["w", "wb", "a", "ab"]:
        raise ValueError("Invalid mode")

    if encoding is not None and not isinstance(encoding, str):
        raise TypeError("Encoding must be a string")

    if os.path.isdir(dest):
        raise IsADirectoryError(f"Destination is a directory: {dest}")

    if not os.path.exists(os.path.dirname(dest)):
        raise FileNotFoundError(f"Destination does not exist: {dest}")

    try:
        with open(dest, mode, encoding=encoding) as f:
            if isinstance(data, bytes) and encoding is None:
                f.write(data)
            else:
                f.write(str(data))
    except (TypeError, ValueError, PermissionError) as e:
        raise
    except Exception as e:
        raise IOError(f"Error writing data to file: {str(e)}") from e


def convert_data_to_string(data, data_format):
    """
    Converts the given data to a string, if it isn't already one.

    :param data: The data to be converted to string. It can be either a string or a bytes object.
    :param data_format: The format of the data. If the format is None, the function tries to infer the format. The format can be either "text" or "binary".
    :raises ValueError: If the data is not in a valid format or cannot be converted to a string.
    :return: The string representation of the data.
    """
    if isinstance(data, str) or (data_format is None and isinstance(data, str)):
        return data
    elif isinstance(data, bytes) or (data_format is None and isinstance(data, bytes)):
        return data.decode("utf-8")
    else:
        raise ValueError("Invalid data format")


def is_generator(obj):
    """
    Checks if the given object is a generator.

    :param obj: The object to be checked.
    :return: True if the object is a generator, False otherwise.
    """
    return hasattr(obj, "__next__")


def validate_arguments(dest: Optional[str], return_data: bool) -> tuple:
    if dest is None and not return_data:
        raise ValueError("dest cannot be None when return_data is False")
    return dest, return_data


def preprocess_duplicate_paths(path_list, duplicate_strategy: DuplicateStrategy):
    paths_to_skip = []
    for i, path in enumerate(path_list):
        if os.path.exists(path) and DuplicateStrategy.SKIP == duplicate_strategy:
            print(f"File already exists: {path}")
            paths_to_skip.append(i)
        elif os.path.exists(path) and DuplicateStrategy.OVERWRITE == duplicate_strategy:
            print(f"Overwriting file: {path}")
        elif os.path.exists(path) and DuplicateStrategy.RENAME == duplicate_strategy:
            path = Path(path)
            j = 1
            while path.exists():
                path = path.with_name(f"{path.stem}_{j}{path.suffix}")
                j += 1
            path_list[i] = str(path)
            print(f"Renaming file to: {path}")
    # Remove paths that are to be skipped
    for i in sorted(paths_to_skip, reverse=True):
        del path_list[i]
    # TODO: This doesn't check if multiple paths are the same
    return path_list


def preprocess_path_list(path_list, create_dirs: bool, duplicate_strategy: DuplicateStrategy):
    if path_list is not None:
        if not isinstance(path_list, list):
            path_list = [path_list]
        for file_path in path_list:
            create_dirs_if_needed(file_path, create_dirs)
        path_list = preprocess_duplicate_paths(path_list, duplicate_strategy)
    return path_list


def save_to_file(data_iter, path_list=None, return_data=True, create_dirs=False, duplicate_strategy=DuplicateStrategy.SKIP, **kwargs):
    """
    Saves the data to a file.

    :param data_iter: The data to be processed. Can be either a list or a generator.
    :param path_list: A list of destination file path. If None and return_data is False, a ValueError is raised.
    :param return_data: A flag indicating whether to return the data or not.
    :param create_dirs: A flag indicating whether to create the directories in the path to the file if they do not exist.
    :param duplicate_strategy: The strategy to be used when a file already exists. If the strategy is DuplicateStrategy.SKIP, the file is skipped. If the strategy is DuplicateStrategy.OVERWRITE, the file is overwritten. If the strategy is DuplicateStrategy.RENAME, the file is renamed.
    :return: The data from the function, or None if return_data is False.
    :raises ValueError: If the destination file does not exist and create_dirs is False, or if an invalid data format is specified.
    :raises OSError: If an error occurs while writing to the file.
    """
    validate_arguments(path_list, return_data)

    path_list = preprocess_path_list(path_list, create_dirs, duplicate_strategy)

    data_to_return = []
    # Raises ValueError if data_iter and path_list have different lengths
    if path_list is not None:
        for path, data in zip(path_list, data_iter, strict=True):
            mode, encoding = determine_mode_and_encoding(data)
            save_data_to_file(data, path, mode, encoding)
            if return_data:
                data_to_return.append(data)
    else:
        # If path_list is None, return_data must be True
        for data in data_iter:
            data_to_return.append(data)

    if return_data:
        if len(data_to_return) == 1:
            return data_to_return[0]
        return data_to_return
