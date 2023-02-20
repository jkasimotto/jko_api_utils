import os

def save_data(data, dest_file=None, return_data=True, create_dirs=False, data_format=None):
    """
    Save data to a file or return it as a string.

    :param data: The data to save or return.
    :param dest_file: The destination file path. If None, the data is returned as a string.
    :param return_data: Whether to return the data as a string. If False, an error is raised if dest_file is None.
    :param create_dirs: Whether to create the directories in dest_file if they don't exist.
    :param data_format: The format of the data (e.g., "text", "binary"). If None, the format is determined automatically.
    :return: If return_data is True, returns the data as a string. Otherwise, returns None.
    """
    if dest_file is not None:
        # Create the directories if they don't exist
        if create_dirs:
            dest_dir = os.path.dirname(dest_file)
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
        elif not os.path.exists(os.path.dirname(dest_file)):
            raise ValueError(f"Destination directory does not exist: {os.path.dirname(dest_file)}")

        # Determine the appropriate method for writing the data to a file based on the data format
        if data_format == "text" or (data_format is None and isinstance(data, str)):
            mode = "w"
            encoding = "utf-8"
        elif data_format == "binary" or (data_format is None and isinstance(data, bytes)):
            mode = "wb"
            encoding = None
        else:
            raise ValueError("Invalid data format")

        # Save the data to the destination file
        with open(dest_file, mode, encoding=encoding) as f:
            if isinstance(data, bytes) and encoding is None:
                f.write(data)
            else:
                f.write(str(data))

    if return_data:
        # Return the data as a string
        if isinstance(data, str) or (data_format is None and isinstance(data, str)):
            return data
        elif isinstance(data, bytes) or (data_format is None and isinstance(data, bytes)):
            return data.decode("utf-8")
        else:
            raise ValueError("Invalid data format")
    else:
        # If return_data is False and dest_file is None, raise an error
        if dest_file is None:
            raise ValueError("dest_file cannot be None when return_data is False")
        return None
