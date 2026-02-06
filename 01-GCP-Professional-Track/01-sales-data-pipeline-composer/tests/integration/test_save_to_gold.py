import pytest

from src.ecommerce_etl.io_factory import IOFactory


@pytest.fixture
def local_manager(tmp_path):
    # We create a local manager pointing to pytest's temporal folder
    return IOFactory.get_manager("local", base_path=str(tmp_path))


def test_save_to_gold_is_idempotent(
    local_manager, sample_valid_partitionable_df, tmp_path
):
    # Test the correct behavior when the process is trigged two times
    target_path = "gold/sales_report"

    # First execution
    local_manager.save_dataframe(sample_valid_partitionable_df, target_path)
    initial_files = local_manager.get_file_list(target_path)

    # Second execution (duplicated)
    local_manager.save_dataframe(sample_valid_partitionable_df, target_path)
    final_files = local_manager.get_file_list(target_path)

    # Assert: There is no extra files or duplicated folders
    assert len(initial_files) == len(final_files)


def test_hive_like_structure(local_manager, sample_valid_partitionable_df, tmp_path):
    # Test the correct tree folder's structure creation
    target_path = "gold/partitioned_data"

    local_manager.save_dataframe(sample_valid_partitionable_df, target_path)

    # Verify physic route exists (Hive style: year=2010/month=12)
    expected_subdir = "gold/partitioned_data/year=2010/month=12"
    files = local_manager.get_file_list(expected_subdir)

    assert len(files) > 0
    assert any(".parquet" in f for f in files)  # O el formato que estés usando
