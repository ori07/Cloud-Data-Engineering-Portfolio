import pytest

from src.ecommerce_etl.data_source import CSVDataSource, LocalMockDataSource
from src.ecommerce_etl.factory import DataSourceFactory, DataSourceTypeError


def test_factory_returns_csv_instance():
    """if the extension is .csv, always must return CSVDataSource."""
    path = "data/test.csv"
    ds = DataSourceFactory.get_data_source(path, mode=".csv")

    assert isinstance(ds, CSVDataSource)
    assert "file" in ds.fs_instance.protocol  # Verificando herencia de set_origin


def test_factory_returns_local_mock():
    # We use an extention different to .csv
    path = "any_file.txt"
    ds = DataSourceFactory.get_data_source(path, mode="Develop-local")

    assert isinstance(ds, LocalMockDataSource)
    assert "file" in ds.fs_instance.protocol


def test_factory_raises_error_unsupported_type():
    with pytest.raises(DataSourceTypeError) as excinfo:
        DataSourceFactory.get_data_source("invalid_file.json", mode=".json")

    assert "Type '.json' not supported" in str(excinfo.value)


@pytest.mark.parametrize(
    "path, mode, expected_protocol",
    [
        ("gs://bucket/data.csv", ".csv", ("gs", "gcs")),
        ("local/path/data.csv", ".csv", ("file", "local")),
    ],
)
def test_factory_integration_with_set_origin(path, mode, expected_protocol):
    """Test that the Factory delivers th object with configured origin."""
    ds = DataSourceFactory.get_data_source(path, mode)
    assert ds.fs_instance.protocol == expected_protocol
