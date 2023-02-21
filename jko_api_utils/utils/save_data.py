import inspect
import os
from functools import wraps
from typing import Optional


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
    :param data_format: The format of the data (e.g., "text", "binary"). If None, the format is determined automatically. # TODO: Enum, json, yaml
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


def save_data_decorator(func):
    """
    A decorator that gives the option to save the data returned by the decorated function to a file.

    :param func: The function to be decorated. Either a generator or a function that returns a string or bytes.
    :return: A wrapper function that saves the data returned by the decorated function to a file.
    :raises TypeError: If the decorated function does not return a string or bytes.
    :raises ValueError: If the destination file is not provided and the return_data flag is False.
    """
    @wraps(func)
    def wrapper(*args, dest=None, return_data=True, create_dirs=False, data_format=None, **kwargs):
        """
        Saves the data returned by the decorated function to a file.

        :param dest: The destination file path. If None and return_data is False, a ValueError is raised.
        :param return_data: A flag indicating whether to return the data or not.
        :param create_dirs: A flag indicating whether to create the directories in the path to the file if they do not exist.
        :param data_format: The format of the data. If None, the format is inferred from the type of the data.
        :return: The data from the function, or None if return_data is False.
        :raises ValueError: If the destination file does not exist and create_dirs is False, or if an invalid data format is specified.
        :raises OSError: If an error occurs while writing to the file.
        """
        validate_arguments(dest, return_data)

        if dest is not None:
            create_dirs_if_needed(dest, create_dirs)

        if inspect.isgeneratorfunction(func):
            results = []
            for result in func(*args, **kwargs):
                mode, encoding = determine_mode_and_encoding(
                    result, data_format)
                save_data_to_file(result, dest, mode, encoding)
                if return_data:
                    results.append(result)
        else:
            result = func(*args, **kwargs)
            mode, encoding = determine_mode_and_encoding(result, data_format)
            save_data_to_file(result, dest, mode, encoding)
            if return_data:
                results = result
        
        if return_data:
            return results
        return None

    return wrapper
