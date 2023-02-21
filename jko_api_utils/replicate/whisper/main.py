import io
import os
from typing import Dict, List, Union

import replicate
from jko_api_utils.utils.save_data import save_data
from tqdm import tqdm


def call_whisper_api(version, filepath, **params):
    # Transcribe the audio
    try:
        result = version.predict(
            audio=open(f"{filepath}", "rb"), **params
        )["transcription"]
    except Exception as e:
        print("Error: ", e)
        result = None

    # Return the result
    return result


def transcribe(target: Union[str, List[str]], dest: str = None, duplicate: str = "skip", return_data=True, create_dirs=False, version=None, **params: Dict) -> List[str]:
    """
    Transcribes one or more audio files or directories of audio files using the openai/whisper model.

    :param target: A file path or a list of file paths to the audio files to transcribe.
    :param dest: The folder where the transcribed files should be saved.
    :param duplicate: What to do if a transcribed file already exists in the `dest` folder. Can be "skip", "overwrite",
                      or "rename".
    :param return_data: Whether to return the data as a string. If False, an error is raised if dest is None.
    :param create_dirs: Whether to create the directories in dest if they don't exist.
    :param version: The version of the openai/whisper model to use. If None, the default version is used.
    :param params: Additional parameters to pass to the transcription model.
    :return: A list of transcribed text strings.
    """
    if version is None:
        version = replicate.models.get("openai/whisper").versions.get(
            "30414ee7c4fffc37e260fcab7842b5be470b9b840f2b608f5baa9bbef9a259ed")

    # Determine the input type of the `target` parameter and call the corresponding function
    if isinstance(target, str) and os.path.isfile(target):
        results = transcribe_files(
            version, [target], dest, duplicate, return_data, create_dirs, **params)
    elif isinstance(target, str) and os.path.isdir(target):
        results = transcribe_dir(
            version, target, dest, duplicate, return_data, create_dirs, **params)
    elif isinstance(target, list) and all([os.path.isfile(item) for item in target]):
        results = transcribe_files(
            version, target, dest, duplicate, return_data, create_dirs, **params)
    elif isinstance(target, list) and all([os.path.isdir(item) for item in target]):
        results = transcribe_dirs(
            version, target, dest, duplicate, return_data, create_dirs, **params)
    else:
        raise ValueError("Invalid input type for `target`")

    return results


def transcribe_file(version, filepath, dest=None, duplicate="skip", return_data=True, create_dirs=False, **params):
    # Transcribe the file
    # File name with text extension
    result = call_whisper_api(version, filepath, **params)
    if dest is not None:
        new_file_name = os.path.basename(filepath).replace(".wav", ".txt")
        new_file_path = os.path.join(dest, new_file_name)
    else:
        new_file_path = None
    return save_data(result, dest_file=new_file_path, return_data=return_data, create_dirs=create_dirs)


def transcribe_files(version, filepaths, dest=None, duplicate="skip", return_data=True, create_dirs=False, **params):
    results = []
    for filepath in tqdm(filepaths, total=len(filepaths), desc="Transcribing files"):
        # Transcribe the file
        result = transcribe_file(
            version, filepath, dest, duplicate, return_data, create_dirs, **params)
        results.append(save_data(result, filepath,
                       return_data=return_data, create_dirs=create_dirs))
    return list(filter(None, results))


def transcribe_dir(version, folder, dest=None, duplicate="skip", return_data=True, create_dirs=False, **params):
    # Get the list of filepaths in the folder
    filepaths = [os.path.join(folder, filename)
                 for filename in os.listdir(folder)]
    return transcribe_files(version, filepaths, dest, duplicate, return_data, create_dirs, **params)


def transcribe_dirs(version, folders, dest=None, duplicate="skip", return_data=True, create_dirs=False, **params):
    results = []
    for folder in tqdm(folders, total=len(folders), desc="Transcribing folders"):
        result = transcribe_dir(version, folder, dest, duplicate, return_data, create_dirs, **params)
        results.extend(result)
    return results
