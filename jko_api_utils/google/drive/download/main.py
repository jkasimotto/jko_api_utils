import io
import os
from pathlib import Path

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from jko_api_utils.BatchAPIController import ApiCaller, BatchAPIController, FilePathGenerator

from jko_api_utils.google.drive.service.get_service import get_service, get_service_with_service_or_secret_path
from jko_api_utils.utils.save_data import DuplicateStrategy, save_to_file


def download(
        drive_folder_id,
        local_folder_path=None,
        max=None,
        exclude=None,
        return_data=True,
        create_dirs=True,
        duplicate_strategy=DuplicateStrategy.SKIP,
        client_secret=None,
        service=None,
):
    """
    Downloads files from a Google Drive folder to a local directory and/or returns file content as a generator.

    Args:
        drive_folder_id (str): The ID of the Google Drive folder to download files from.
        local_folder_path (str or Path, optional): The local directory to download files to. Defaults to None.
        max (int, optional): The maximum number of files to download. Defaults to None.
        exclude (list of str, optional): A list of file names to exclude from the download. Defaults to None.
        return_data (bool, optional): Whether to return file content as a generator. Defaults to True.
        create_dirs (bool, optional): Whether to create the local directory if it doesn't exist. Defaults to True.
        duplicate_strategy (DuplicateStrategy, optional): The strategy to use for files that already exist in the local directory. Defaults to DuplicateStrategy.SKIP.
        client_secret (str or Path, optional): The path to the Google Drive client secret file. Required if service is not provided. Defaults to None.
        service (googleapiclient.discovery.Resource, optional): An authenticated Google Drive API service object. Required if client_secret is not provided. Defaults to None.

    Yields:
        list of bytes: A list of file content bytes, if return_data is True.

    Raises:
        ValueError: If both client_secret and service are None.
    """
    service = get_service_with_service_or_secret_path(service, client_secret)
    local_folder_path = Path(
        local_folder_path) if local_folder_path is not None else None

    # Give the user the option to create the local folder if it doesn't exist
    if local_folder_path is not None and create_dirs:
        local_folder_path.mkdir(parents=True, exist_ok=True)

    # Get the files in the folder
    file_count = 0
    file_batch_contents = []
    file_batch_gen = gen_files_in_folder(
        service, drive_folder_id, batch_size=100)
    for file_batch in file_batch_gen:
        for file in file_batch:

            # Give the user the option to exclude files from the download
            if exclude is not None and file['name'] in exclude:
                continue

            # Give the user the option to skip, rename or overwrite files that already exist
            if local_folder_path is not None:
                file_path = local_folder_path / file['name']
                if file_path.exists():
                    if duplicate_strategy == DuplicateStrategy.SKIP:
                        print(f"Skipping {file['name']}")
                        continue
                    elif duplicate_strategy == DuplicateStrategy.RENAME:
                        k = 1
                        while file_path.exists():
                            file_path = file_path.with_name(
                                f"{file_path.stem}_{k}{file_path.suffix}")
                            k += 1
                    elif duplicate_strategy == DuplicateStrategy.OVERWRITE:
                        pass

            # Give the user the option to limit the number of files downloaded
            if max is not None and file_count >= max:
                yield file_batch_contents
                break

            # Download the file content
            file_content = get_file_content(service, file['id'])
            file_count += 1

            # Give the user the option to download to a local folder
            # The file_path variable is set above
            if local_folder_path is not None:
                with open(file_path, "wb") as f:
                    f.write(file_content)

            # Only return the data if return_data is True otherwise it may consume too much memory.
            if return_data:
                file_batch_contents.append(file_content)

        yield file_batch_contents


def gen_files_in_folder(service, folder_id, batch_size=100):
    """Returns a generator that yields batches of files in a Google Drive folder.

    Args:
        service (googleapiclient.discovery.Resource): An authorized Drive API service instance.
        folder_id (str): The ID of the Google Drive folder to query.
        batch_size (int, optional): The number of files to return in each batch.

    Yields:
        A batch of files in the specified folder.
    """

    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query, fields="nextPageToken, files(id, name, mimeType, size)", pageSize=batch_size).execute()
    while True:
        items = results.get('files', [])
        yield items
        next_page_token = results.get('nextPageToken', None)
        if next_page_token is None:
            break
        results = service.files().list(q=query, fields="nextPageToken, files(id, name, mimeType, size)",
                                       pageToken=next_page_token, pageSize=batch_size).execute()


def get_file_content(service, file_id, mime_type=None):
    """Gets the media content of a file in Google Drive.

    Args:
        service (googleapiclient.discovery.Resource): An authorized Drive API service instance.
        file_id (str): The ID of the file to get the media content for.
        mimeType (str, optional): The MIME type of the exported file if it is a Google Workspace document.

    Returns:
        The media content of the file as a byte string.
    """
    # Check the MIME type of the file
    file_metadata = service.files().get(fileId=file_id, fields="mimeType").execute()
    file_mimetype = file_metadata.get("mimeType", "")

    # If the file is a Google Workspace document, export it as the specified MIME type
    if file_mimetype.startswith("application/vnd.google-apps"):
        if mime_type is None:
            raise ValueError(
                "MIME type must be specified for Google Workspace documents")
        content = export_google_workspace_document(service, file_id, mime_type)
    else:
        # Otherwise, download the file content
        request = service.files().get_media(fileId=file_id)
        content = io.BytesIO()
        downloader = MediaIoBaseDownload(content, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        content.seek(0)

    return content.read()


def export_google_workspace_document(service, file_id, mimeType):
    """Exports a Google Workspace document in the specified mimeType.

    Args:
        service (googleapiclient.discovery.Resource): An authorized Drive API service instance.
        file_id (str): The ID of the Google Workspace document to export.
        mimeType (str): The MIME type of the exported file.

    Returns:
        The media content of the exported file.
    """
    try:
        # Export the file
        request = service.files().export_media(fileId=file_id, mimeType=mimeType)

        # Get the media content
        content = io.BytesIO()
        downloader = MediaIoBaseDownload(content, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.")
        content.seek(0)
        return content.read()
    except HttpError as e:
        print(f"An error occurred: {e}")
        content = None