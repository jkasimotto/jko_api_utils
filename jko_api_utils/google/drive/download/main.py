import io
import os

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from jko_api_utils.google.drive.service.get_service import get_service
from jko_api_utils.utils.save_data import DuplicateStrategy, save_to_file_decorator


@save_to_file_decorator
def download(drive_folder_id, client_secret=None, service=None, max=None, exclude=None, path_list=None, return_data=True, create_dirs=True, duplicate_strategy=DuplicateStrategy.SKIP):
    """Downloads files from a Google Drive folder.

    Args:
        drive_folder_id (str): The ID of the Google Drive folder to download files from.
        client_secret (str, optional): The path to a JSON file containing the client secret for authentication.
        service (googleapiclient.discovery.Resource, optional): An authorized Drive API service instance. If None, one will be created from the client_secret.
        max (int, optional): The maximum number of files to download. If None, downloads all files.
        exclude (list, optional): A list of filenames to exclude from the download.
        path_list: A list of destination file path. If None and return_data is False, a ValueError is raised.
        return_data: A flag indicating whether to return the data or not.
        create_dirs: A flag indicating whether to create the directories in the path to the file if they do not exist.
        duplicate_strategy: The strategy to be used when a file already exists. If the strategy is DuplicateStrategy.SKIP, the file is skipped. If the strategy is DuplicateStrategy.OVERWRITE, the file is overwritten. If the strategy is DuplicateStrategy.RENAME, the file is renamed.

    Yields:
        The contents of each downloaded file as a byte string.
    """
    if service is None and client_secret is None:
        raise ValueError(
            "Either client_secret or service must be specified.")
    elif service is None:
        service = get_service(
            client_secret, ['https://www.googleapis.com/auth/drive.readonly'])

    files = get_files_in_folder(service, drive_folder_id, max)

    for file in files:
        if exclude is not None and file['name'] in exclude:
            continue
        yield get_file_content(service, file)


def get_files_in_folder(service, folder_id, max=None):
    """Returns a list of files in a Google Drive folder.

    Args:
        service (googleapiclient.discovery.Resource): An authorized Drive API service instance.
        folder_id (str): The ID of the Google Drive folder to query.
        max (int, optional): The maximum number of files to return. If None, returns all files.

    Returns:
        A list of files in the specified folder.
    """

    files = []
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query, fields="nextPageToken, files(id, name, mimeType, size)").execute()
    while True:
        items = results.get('files', [])
        files.extend(items)
        next_page_token = results.get('nextPageToken', None)
        if next_page_token is None or (max is not None and len(files) >= max):
            break
        results = service.files().list(q=query, fields="nextPageToken, files(id, name, mimeType, size)",
                                       pageToken=next_page_token).execute()

    return files if max is None else files[:max]


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
