import io
import os

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from jko_api_utils.google.drive.service.get_service import get_service


def download(
    client_secret, 
    drive_folder_id, 
    dest_dir=None, 
    max=None, 
    duplicate="skip", 
    exclude=None
):
    service = get_service(
        client_secret, 
        ['https://www.googleapis.com/auth/drive.readonly']
    )
    files = get_files_in_folder(service, drive_folder_id, max)

    results = download_file_content(
        service, 
        files, 
        dest_dir, 
        max,
        duplicate, 
        exclude
        )
    return results


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


def download_file_content(service, files, dest_dir=None, max=None, duplicate="skip", exclude=None, mime_type=None):
    """Downloads files from a Google Drive folder.

    Args:
        service (googleapiclient.discovery.Resource): An authorized Drive API service instance.
        files (list): A list of files to download.
        dest_dir (str, optional): The local destination directory to download files to. If None, returns the downloaded files.
        max (int, optional): The maximum number of files to download. If None, downloads all files.
        duplicate (str, optional): How to handle duplicate file names. Must be one of 'skip', 'overwrite', or 'rename'.
        exclude (list, optional): A list of file names to exclude from the download.

    Returns:
        A list of file paths of the downloaded files if to is not None, otherwise a list of byte strings of the downloaded files.
    """
    downloaded_files = []
    for file in files:
        if exclude is not None and file['name'] in exclude:
            continue
        if max is not None and len(downloaded_files) >= max:
            break
        if dest_dir is None:
            # If to is None, return the downloaded files as byte strings
            content = get_file_content(
                service, file['id'], mime_type=mime_type)
            downloaded_files.append(content)
        else:
            # If to is not None, download the files to the local destination directory first checking for duplicate file names
            file_name = file['name']
            file_path = os.path.join(dest_dir, file_name)
            # Handle duplicate file names
            if os.path.exists(file_path):
                new_path = handle_duplicate(file_path, duplicate)
                if new_path is None:
                    continue
                file_path = new_path
            # Write the content to the file
            content = get_file_content(
                service, file['id'], mime_type=mime_type)
            with open(file_path, "wb") as f:
                f.write(content)
    return downloaded_files


def handle_duplicate(file_path, duplicate):
    """Handles duplicate files.

    Args:
        file_path (str): The file path to handle.
        duplicate (str): How to handle duplicate file names. Must be one of 'skip', 'overwrite', or 'rename'.

    Returns:
        A new file path with a modified name if duplicate is 'rename', otherwise None.
    """
    duplicate_functions = {
        "skip": handle_duplicate_skip,
        "overwrite": handle_duplicate_overwrite,
        "rename": handle_duplicate_rename
    }
    if duplicate not in duplicate_functions:
        raise ValueError(
            f"Duplicate setting '{duplicate}' is not valid. Must be one of 'skip', 'overwrite', or 'rename'."
        )
    else:
        return duplicate_functions[duplicate](file_path)


def handle_duplicate_skip(file_path):
    """Handles the 'skip' duplicate setting.

    Args:
        file_path (str): The file path to handle.

    Returns:
        None
    """
    return


def handle_duplicate_overwrite(file_path):
    """Handles the 'overwrite' duplicate setting.

    Args:
        file_path (str): The file path to handle.

    Returns:
        None
    """
    os.remove(file_path)


def handle_duplicate_rename(file_path):
    """Handles the 'rename' duplicate setting.

    Args:
        file_path (str): The file path to handle.

    Returns:
        A new file path with a modified name.
    """
    file_name, file_extension = os.path.splitext(file_path)
    index = 1
    while os.path.exists(file_path):
        file_name = f"{file_name} ({index})"
        file_path = os.path.join(to, f"{file_name}{file_extension}")
        index += 1
    return file_path


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
        if mimeType is None:
            raise ValueError(
                "MIME type must be specified for Google Workspace documents")
        content = export_google_workspace_document(service, file_id, mimeType)
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
