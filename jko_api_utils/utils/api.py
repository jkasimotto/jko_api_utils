import os


def call_fn_and_save_data(fn, file_path, overwrite=False, append=False, *args, **kwargs):
    if not overwrite and os.path.exists(file_path):
        return  # file exists and overwrite is False, so do not call the function

    response = fn(*args, **kwargs)
    if response:
        if append:
            mode = 'a'
        else:
            mode = 'w'
        with open(file_path, mode) as f:
            f.write(response)
    else:
        print("Error calling the function.")
