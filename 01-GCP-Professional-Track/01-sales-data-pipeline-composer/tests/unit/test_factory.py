import pytest

# Importing trigger @register decorators
from src.ecommerce_etl.factory import DataSourceFactory, DataSourceTypeError


def test_registry_contains_expected_sources():
    """Verify the source register then by themselfs when the module was imported."""
    registered_keys = DataSourceFactory._registry.keys()

    assert ".csv" in registered_keys
    assert "Develop-local" in registered_keys


def test_factory_creates_correct_instance_from_registry():
    """Verify that Factory delivers the correct class associated to the register."""
    path = "gs://my-bucket-cloud/data.csv"
    instance = DataSourceFactory.get_data_source(path, mode=".csv")

    from src.ecommerce_etl.data_source import CSVDataSource

    assert isinstance(instance, CSVDataSource)
    assert "gs" in instance.fs_instance.protocol  # set_origin works


def test_should_raise_error_for_unregistered_mode():
    path = "gs://my-bucket-cloud/data.json"
    # Assert
    with pytest.raises(DataSourceTypeError):
        DataSourceFactory.get_data_source(path, mode=".json")
