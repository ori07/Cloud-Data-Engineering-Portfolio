import os

import fsspec as fs
import pytest

from config import config
from src.ecommerce_etl.io_factory import IOFactory


@pytest.fixture
def gcs_setup_teardown():
    # Setup: Define a unique test prefix to avoid collisions
    test_prefix = f"{config.GOLD_PATH_TEST}/integration_test_tmp"
    gcs = fs.filesystem("gs")
    clean_prefix = test_prefix.replace("gs://", "")
    yield test_prefix
    # Teardown: It is ALWAYS executed, fail or not the test
    try:
        if gcs.exists(clean_prefix):
            gcs.rm(clean_prefix, recursive=True)
    except Exception as e:
        print(f"Cleanup failed (expected if folder wasn't created): {e}")


@pytest.fixture
def cloud_manager():
    # We create a cloud manager pointing to pytest's temporal folder
    bucket_name = config.GOLD_PATH_TEST
    manager = IOFactory.get_manager(bucket_name)
    return manager


@pytest.mark.skipif(
    os.environ.get("GOLD_OUTPUT_PATH") is None, reason="GCS bucket path not configured"
)
def test_gcs_persistence_integration(
    cloud_manager, sample_valid_partitionable_df, gcs_setup_teardown
):
    path = gcs_setup_teardown
    # Save the df
    cloud_manager.save_dataframe(sample_valid_partitionable_df, path)

    # Verification, using fsspec abstraction
    files = cloud_manager.get_file_list(path)

    assert len(files) > 0
    assert any("year=2010" in f for f in files)
