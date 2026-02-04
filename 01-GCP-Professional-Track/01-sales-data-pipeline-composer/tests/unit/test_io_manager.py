from unittest.mock import MagicMock

import pytest

from src.ecommerce_etl.io_manager import (
    CloudIOManager,
    IOManagerError,
    IOManagerPermissionsError,
    LocalIOManager,
)


# Test for CloudManager
def test_cloud_manager_exists(mock_fsspec):
    mock_fsspec.exists.return_value = True
    manager = CloudIOManager(project="test-project")

    assert manager.exists("gs://bucket/file.csv") is True
    mock_fsspec.exists.assert_called_once_with("gs://bucket/file.csv")


def test_cloud_manager_exists_raises_error(mock_fsspec):
    mock_fsspec.exists.side_effect = Exception("Connection Timeout")
    manager = CloudIOManager()

    with pytest.raises(IOManagerError, match="File not found at"):
        manager.exists("gs://bucket/file.csv")


def test_cloud_manager_get_file_list(mock_fsspec):
    mock_fsspec.find.return_value = ["file1.parquet", "file2.parquet"]
    manager = CloudIOManager()
    files = manager.get_file_list("gs://bucket/folder")

    assert len(files) == 2
    assert "file1.parquet" in files


def test_cloud_manager_open_stream(mock_fsspec):
    mock_stream = MagicMock()
    mock_fsspec.open.return_value = mock_stream

    manager = CloudIOManager()
    stream = manager.open_stream("gs://bucket/data.csv")

    assert stream == mock_stream
    mock_fsspec.open.assert_called_once_with("gs://bucket/data.csv", mode="rb")


def test_cloud_manager_open_stream_permission_denied(mock_fsspec):
    mock_fsspec.open.side_effect = Exception(
        "403 Forbidden: user does not have storage.objects.get access"
    )

    manager = CloudIOManager()

    # Verificamos que tu código captura el error y lanza TU excepción personalizada
    with pytest.raises(
        IOManagerPermissionsError, match="Pemission denied to read file"
    ):
        manager.open_stream("gs://bucket/secret_data.csv")


# Test for LocalManager
def test_local_manager_exists(mock_fsspec):
    mock_fsspec.exists.return_value = True

    manager = LocalIOManager()

    assert manager.exists("file:///tmp/data/file.csv") is True
    mock_fsspec.exists.assert_called_once_with("file:///tmp/data/file.csv")


def test_local_manager_exists_raises_error(mock_fsspec):
    mock_fsspec.exists.side_effect = Exception("File Corrupt")

    manager = LocalIOManager()

    with pytest.raises(IOManagerError, match="File not found at"):
        manager.exists("file:///tmp/file.csv")


def test_local_manager_get_file_list(mock_fsspec):
    mock_fsspec.find.return_value = ["file1.parquet", "file2.parquet"]

    manager = LocalIOManager()
    files = manager.get_file_list("file:///tmp/data/")

    assert len(files) == 2
    assert "file1.parquet" in files


def test_local_manager_open_stream(mock_fsspec):
    mock_stream = MagicMock()
    mock_fsspec.open.return_value = mock_stream

    manager = CloudIOManager()
    stream = manager.open_stream("file:///local/data.csv")

    assert stream == mock_stream
    mock_fsspec.open.assert_called_once_with("file:///local/data.csv", mode="rb")
