from src.ecommerce_etl import io_manager


def test_save_to_gold_is_idempotent(tmp_path, sample_valid_partitionable_df):
    # Test the correct behavior when the process is trigged two times
    base_path = tmp_path / "gold"

    # First execution
    io_manager.save_to_gold(sample_valid_partitionable_df, base_path)
    initial_files = io_manager.get_file_list(base_path)

    # Second execution (duplicated)
    io_manager.save_to_gold(sample_valid_partitionable_df, base_path)
    final_files = io_manager.get_file_list(base_path)

    # Assert: There is no extra files or duplicated folders
    assert len(initial_files) == len(final_files)


def test_hive_like_structure(sample_valid_partitionable_df, tmp_path):
    # Test the correct tree folder's structure creation
    base_path = tmp_path / "gold"

    # Save the df
    io_manager.save_to_gold(sample_valid_partitionable_df, base_path)
    expected_path = base_path / "year=2010" / "month=12"
    files = io_manager.get_file_list(expected_path)
    print(files)

    # There is any file in the expected path
    assert len(files) > 0
