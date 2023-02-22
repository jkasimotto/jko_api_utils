from typing import List, Optional, Union
import os
from typing import List, Union

import replicate


def batch_transcribe(inputs: List[Union[str, bytes]]) -> List[str]:
    """Batch transcribe a list of audio files or raw audio data inputs using the Whisper API.

    Args:
        inputs: A list of file paths or raw audio data inputs to transcribe.

    Returns:
        A list of transcriptions in the same order as the input list.
    """
    if not inputs:
        raise ValueError("Inputs list cannot be empty.")

    transcriptions = []

    for input in enumerate(inputs):
        # Check if input is file path or raw audio data
        if isinstance(input, str):
            transcription = _transcribe_file(input)
        else:
            transcription = _transcribe_raw(input)
        transcriptions.append(transcription)

    return transcriptions


def get_whisper_version(version=None):
    model = replicate.models.get("openai/whisper")
    if version is None:
        version = model.versions.get(
            "30414ee7c4fffc37e260fcab7842b5be470b9b840f2b608f5baa9bbef9a259ed")
    else:
        version = model.versions.get(version)
    return version


def _call_whisper_api(audio, version=None, model="base"):
    if version is None:
        version = get_whisper_version()
    # Transcribe the audio
    try:
        result = version.predict(
            audio=audio,
            model=model
        )["transcription"]
    except Exception as e:
        print("Error: ", e)
        result = None

    # Return the result
    return result


def _transcribe_file(input_file: str) -> str:
    """Transcribe a single audio file using the Whisper API.

    Args:
        input_file: The file path of the audio file to transcribe.

    Returns:
        The transcription output as a string.
    """
    with open(input_file, "rb") as f:
        audio_data = f.read()
    transcription = _call_whisper_api(audio_data)
    return transcription


def _transcribe_raw(input_data: bytes) -> str:
    """Transcribe a single audio input in raw audio data format using the Whisper API.

    Args:
        input_data: The raw audio data to transcribe.

    Returns:
        The transcription output as a string.
    """
    transcription = _call_whisper_api(input_data)
    return transcription
