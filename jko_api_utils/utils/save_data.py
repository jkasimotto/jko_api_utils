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


def determine_mode_and_encoding(data, data_format, append=False):
    """
    Determines the appropriate mode and encoding for writing the given data to a file based on its format.

    :param data: The data to be written to the file.
    # TODO: Enum, json, yaml
    :param data_format: The format of the data (e.g., "text", "binary"). If None, the format is determined automatically.
    :param append: Whether to append to the file or overwrite it.
    :return: A tuple of the mode and encoding to use for writing the data to a file.
    :raises ValueError: If the data format is invalid.
    """
    if append:
        mode = "a" if data_format == "text" or (
            data_format is None and isinstance(data, str)) else "ab"
        encoding = "utf-8" if data_format == "text" or (
            data_format is None and isinstance(data, str)) else None
    else:
        mode = "w" if data_format == "text" or (
            data_format is None and isinstance(data, str)) else "wb"
        encoding = "utf-8" if data_format == "text" or (
            data_format is None and isinstance(data, str)) else None

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
    return path_list


def process_path_list(path_list, create_dirs: bool, duplicate_strategy: DuplicateStrategy):
    if path_list is not None:
        if not isinstance(path_list, list):
            path_list = [path_list]
        for file_path in path_list:
            create_dirs_if_needed(file_path, create_dirs)
        path_list = preprocess_duplicate_paths(path_list, duplicate_strategy)
    return path_list


def save_to_file_decorator(func):
    """
    A decorator that gives the option to save the data returned by the decorated function to a file.

    :param func: The function to be decorated. Either a generator or a function that returns a string or bytes.
    :return: A wrapper function that saves the data returned by the decorated function to a file.
    :raises TypeError: If the decorated function does not return a string or bytes.
    :raises ValueError: If the destination file is not provided and the return_data flag is False.
    """
    @wraps(func)
    def wrapper(*args, path_list=None, return_data=True, create_dirs=False, duplicate_strategy=DuplicateStrategy.SKIP, **kwargs):
        """
        Saves the data returned by the decorated function to a file.

        :param path_list: A list of destination file path. If None and return_data is False, a ValueError is raised.
        :param return_data: A flag indicating whether to return the data or not.
        :param create_dirs: A flag indicating whether to create the directories in the path to the file if they do not exist.
        :param data_format: The format of the data. If None, the format is inferred from the type of the data.
        :param duplicate_strategy: The strategy to be used when a file already exists. If the strategy is DuplicateStrategy.SKIP, the file is skipped. If the strategy is DuplicateStrategy.OVERWRITE, the file is overwritten. If the strategy is DuplicateStrategy.RENAME, the file is renamed.
        :return: The data from the function, or None if return_data is False.
        :raises ValueError: If the destination file does not exist and create_dirs is False, or if an invalid data format is specified.
        :raises OSError: If an error occurs while writing to the file.
        """
        validate_arguments(path_list, return_data)

        path_list = process_path_list(path_list, create_dirs, duplicate_strategy)

        result = func(*args, **kwargs)
        if isinstance(result, str) or isinstance(result, bytes):
            results = [result]
        elif is_generator(result):
            results = result
        else:
            raise TypeError(
                "The decorated function must return a string or bytes, or an iterable.")

        data_to_return = []
        # Raises ValueError if path_list and results have different lengths
        if path_list is not None:
            for dest, result in zip(path_list, results, strict=True):
                mode, encoding = determine_mode_and_encoding(result, data_format)
                save_data_to_file(result, dest, mode, encoding)
                if return_data:
                    data_to_return.append(result)
        else:
            # If path_list is None, return_data must be True
            for result in results:
                data_to_return.append(result)

        if return_data:
            if len(data_to_return) == 1:
                return data_to_return[0]
            return data_to_return

    return wrapper


def handle_duplicate_decorator(func):
    @wraps(func)
    def wrapper(*args, path_list=None, duplicates_strategy=DuplicateStrategy.SKIP, **kwargs):
        if path_list is None:
            raise ValueError("The 'path_list' parameter must be set.")
        if not isinstance(path_list, list):
            path_list = [path_list]
        for i in range(len(path_list)):
            path = Path(path_list[i])
            if path.exists():
                if duplicates_strategy == DuplicateStrategy.RENAME:
                    j = 1
                    while True:
                        path = path.with_name(f"{path.stem}_{j}{path.suffix}")
                        if not path.exists():
                            path_list[i] = str(path)
                            break
                        j += 1
                elif duplicates_strategy == DuplicateStrategy.SKIP:
                    continue
        return func(*args, path_list=path_list, **kwargs)
    return wrapper
