from src.ecommerce_etl import io_manager


def test_anomaly_exclusion(sample_valid_enriched_df):
    # Test non row flaaged as "Anomaly" is part of the df
    df = sample_valid_enriched_df
    anomalies = io_manager.discard_anomalies(df)[0]
    assert len(anomalies) == 0


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
