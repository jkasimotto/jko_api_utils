import click
from replicate_api_utils.whisper.main import transcribe

@click.command()
@click.argument("target")
@click.option("--to", "-o", default=None, help="Output directory for transcriptions. Specify None to return the transcriptions.")
@click.option("--duplicate", "-d", default="skip", type=click.Choice(["skip", "rename", "overwrite"]), help="What to do if an output file already exists.")
@click.option("--model", default="base", type=click.Choice(["tiny", "base", "small", "medium", "large-v1", "large-v2"]), help="The model to use for transcription.")
@click.option("--transcription", default="plain text", type=click.Choice(["plain text", "srt", "vtt"]), help="The format for the transcription.")
@click.option("--translate", is_flag=True, help="Translate the text to English when set to True.")
@click.option("--language", default=None, type=str, help="Language spoken in the audio, specify None to perform language detection.")
def main(target, to, duplicate, model, transcription, translate, language):
    # Map the command-line options to the parameters of the transcribe function
    params = {
        "model": model,
        "transcription_format": transcription,
        "translate": translate,
        "language": language
    }

    # Call the transcribe function with the specified options
    transcribe(target, to=to, duplicate=duplicate, **params)


if __name__ == "__main__":
    main()
