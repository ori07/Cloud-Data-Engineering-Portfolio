import pytest

# Importing trigger @register decorators
from src.ecommerce_etl.data_source_factory import DataSourceFactory, DataSourceTypeError


def test_registry_contains_expected_sources():
    """Verify the source register then by themselfs when the module was imported."""

    # Importing the classes to fill in the factoy's register
    from src.ecommerce_etl.data_source import (
        CSVDataSource,  # noqa: F401
        ParquetDataSource,  # noqa: F401
    )

    registered_keys = DataSourceFactory._registry.keys()

    assert ".csv" in registered_keys
    assert ".parquet" in registered_keys


@pytest.mark.parametrize(
    "path, mode, expected_class_name",
    [
        (
            "gs://bucket/file.csv",
            ".csv",
            "CSVDataSource",
        ),
        ("local/path/file.parquet", None, "ParquetDataSource"),
    ],
)
def test_factory_creates_correct_instance_from_registry(
    path, mode, expected_class_name
):
    """Verify that Factory delivers the correct class associated to the register."""
    data_source = DataSourceFactory.get_data_source(path, mode)

    assert data_source.__class__.__name__ == expected_class_name


def test_should_raise_error_for_unregistered_mode():
    path = "gs://my-bucket-cloud/data.json"
    # Assert
    with pytest.raises(DataSourceTypeError):
        DataSourceFactory.get_data_source(path, mode=".json")
