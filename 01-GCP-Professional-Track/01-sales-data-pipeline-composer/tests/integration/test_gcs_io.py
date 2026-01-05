import fsspec as fs
import pytest

from config import config
from src.ecommerce_etl import io_manager

# @pytest.fixture(scope="session", autouse=True)
# def set_env_variables(monkeypatch):
#    # Example of setting a variable
#    monkeypatch.setenv("GOLD_PATH", config.GOLD_PATH)


@pytest.fixture
def gcs_setup_teardown():
    # Setup: Define a unique test prefix to avoid collisions
    test_prefix = f"{config.GOLD_PATH}/integration_test_tmp"
    yield test_prefix
    # Teardown: It is ALWAYS executed, fail or not the test
    fs_intance, _ = fs.core.url_to_fs(test_prefix)
    if fs_intance.exists(test_prefix):
        fs_intance.rm(test_prefix, recursive=True)


# @pytest.mark.skipif(
#    os.environ.get("GOLD_OUTPUT_PATH") is None, reason="GCS bucket path not configured"
# )
def test_gcs_persistence_integration(sample_valid_partitionable_df, gcs_setup_teardown):
    path = gcs_setup_teardown
    # Save the df
    io_manager.save_to_gold(sample_valid_partitionable_df, path)

    # Verification, using fsspec abstraction
    files = io_manager.list_gcs_files(path)

    assert len(files) > 0
    assert any("year=2010" in f for f in files)
