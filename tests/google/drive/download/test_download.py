import io
import pytest
import tempfile
import os
from unittest import mock
from jko_api_utils.google.drive.download import download


@mock.patch('jko_api_utils.google.drive.download.main.get_file_content')
@mock.patch('jko_api_utils.google.drive.download.main.gen_files_in_folder')
def test_download_only_return(mock_gen_files_in_folder, mock_get_file_content):
    # set up mock values for service, files, and content
    mock_service = mock.MagicMock()
    mock_file_1 = {'id': 'file1_id', 'name': 'file1.txt',
                   'mimeType': 'text/plain', 'size': 100}
    mock_file_2 = {'id': 'file2_id', 'name': 'file2.png',
                   'mimeType': 'image/png', 'size': 200}
    mock_files = [mock_file_1, mock_file_2]
    mock_content_1 = b'file1 contents'
    mock_content_2 = b'file2 contents'

    # set up the mocks
    mock_gen_files_in_folder.return_value = (f for f in [mock_files])
    mock_get_file_content.side_effect = [mock_content_1, mock_content_2]

    # run the function
    result = list(download('folder_id', service=mock_service))[0]

    # check the results
    mock_get_file_content.assert_any_call(mock_service, 'file1_id')
    mock_get_file_content.assert_any_call(mock_service, 'file2_id')
    assert result == [mock_content_1, mock_content_2]


@mock.patch('jko_api_utils.google.drive.download.main.get_file_content')
@mock.patch('jko_api_utils.google.drive.download.main.gen_files_in_folder')
def test_download_with_local_path(mock_gen_files_in_folder, mock_get_file_content):
    # set up mock values for service, files, and content
    mock_service = mock.MagicMock()
    mock_file_1 = {'id': 'file1_id', 'name': 'file1.txt',
                   'mimeType': 'text/plain', 'size': 100}
    mock_file_2 = {'id': 'file2_id', 'name': 'file2.png',
                   'mimeType': 'image/png', 'size': 200}
    mock_files = [mock_file_1, mock_file_2]
    mock_content_1 = b'file1 contents'
    mock_content_2 = b'file2 contents'

    # set up the mocks
    mock_gen_files_in_folder.return_value = (f for f in [mock_files])
    mock_get_file_content.side_effect = [mock_content_1, mock_content_2]

    # create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        # run the function
        gen = download('folder_id', local_folder_path=temp_dir, service=mock_service)
        for g in gen:
            pass

        # check that the files were downloaded to the correct location
        file_path_1 = os.path.join(temp_dir, 'file1.txt')
        file_path_2 = os.path.join(temp_dir, 'file2.png')
        assert os.path.exists(file_path_1)
        assert os.path.exists(file_path_2)
        with open(file_path_1, 'rb') as f:
            assert f.read() == mock_content_1
        with open(file_path_2, 'rb') as f:
            assert f.read() == mock_content_2

@mock.patch('jko_api_utils.google.drive.download.main.get_file_content')
@mock.patch('jko_api_utils.google.drive.download.main.gen_files_in_folder')
def test_download_with_exclude(mock_gen_files_in_folder, mock_get_file_content):
    # set up mock values for service, files, and content
    mock_service = mock.MagicMock()
    mock_file_1 = {'id': 'file1_id', 'name': 'file1.txt', 'mimeType': 'text/plain', 'size': 100}
    mock_file_2 = {'id': 'file2_id', 'name': 'file2.png', 'mimeType': 'image/png', 'size': 200}
    mock_files = [mock_file_1, mock_file_2]
    mock_content_1 = b'file1 contents'
    mock_content_2 = b'file2 contents'

    # set up the mocks
    mock_gen_files_in_folder.return_value = (f for f in [mock_files])
    mock_get_file_content.side_effect = [mock_content_1, mock_content_2]

    # run the function with an exclusion list
    result = list(download('folder_id', exclude=['file2.png'], service=mock_service))[0]

    # check that the excluded file was not downloaded
    assert mock_get_file_content.call_count == 1
    mock_get_file_content.assert_called_with(mock_service, 'file1_id')
    assert result == [mock_content_1]