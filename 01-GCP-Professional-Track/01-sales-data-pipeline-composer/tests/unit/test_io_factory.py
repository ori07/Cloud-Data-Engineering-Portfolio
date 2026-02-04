import pytest

# Importing trigger @register decorators
from src.ecommerce_etl.io_factory import IOFactory, IOProtocolError


# Behavioral testing
def test_registry_contains_expected_io_managers():
    """Verify the manager register by themselfs when the module was imported."""

    registered_keys = IOFactory._registry.keys()

    assert "gs" in registered_keys
    assert "file" in registered_keys


@pytest.mark.parametrize(
    "uri, config, expected_class_name",
    [
        (
            "gs://bucket/file.csv",
            {"token": "abc-123", "project": "my-project"},
            "CloudIOManager",
        ),
        ("local/path/file.csv", {}, "LocalIOManager"),
    ],
)
def test_factory_creates_correct_instance_from_registry(
    uri, config, expected_class_name
):
    """Verify that Factory delivers the correct class associated to the register."""

    manager = IOFactory.get_manager(uri, **config)

    assert manager.__class__.__name__ == expected_class_name


def test_for_unregistered_mode():
    path = "tcp://my-nonexistenthost:1521/fakeservicename"
    test_config = {"user": "my_user", "password": "abc-123", "role": "db_admin"}
    # Assert
    with pytest.raises(IOProtocolError):
        IOFactory.get_manager(path, **test_config)
