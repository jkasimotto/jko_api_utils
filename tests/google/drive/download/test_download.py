import io
import pytest
from unittest import mock
from jko_api_utils.google.drive.download import download

@mock.patch('jko_api_utils.google.drive.download.main.get_files_in_folder')
@mock.patch('jko_api_utils.google.drive.download.main.get_file_content')
def test_download(mock_get_file_content, mock_get_files_in_folder):
    # set up mock values for service, files, and content
    mock_service = mock.MagicMock()
    mock_file_1 = {'id': 'file1_id', 'name': 'file1.txt', 'mimeType': 'text/plain', 'size': 100}
    mock_file_2 = {'id': 'file2_id', 'name': 'file2.png', 'mimeType': 'image/png', 'size': 200}
    mock_files = [mock_file_1, mock_file_2]
    mock_content_1 = b'file1 contents'
    mock_content_2 = b'file2 contents'

    # set up the mocks
    mock_get_files_in_folder.return_value = mock_files
    mock_get_file_content.side_effect = [mock_content_1, mock_content_2]

    # run the function
    result = list(download('folder_id', service=mock_service))

    # check the results
    mock_get_files_in_folder.assert_called_once_with(mock_service, 'folder_id', max=None)
    mock_get_file_content.assert_any_call(mock_service, 'file1_id')
    mock_get_file_content.assert_any_call(mock_service, 'file2_id')
    assert result == [mock_content_1, mock_content_2]
