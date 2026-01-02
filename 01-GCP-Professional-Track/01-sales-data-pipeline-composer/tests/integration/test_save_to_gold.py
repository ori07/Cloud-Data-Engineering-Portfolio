from src.ecommerce_etl import save_to_gold


def test_date_column_type(sample_enriched_df):
    # Test if the column with the date in dataframe has te correct type
    df = save_to_gold.convert_date_column(sample_enriched_df)
    actual_type = df["InvoiceDate"].dtype
    expected_type = "datetime64[ns]"

    assert actual_type == expected_type


def test_anomaly_exclusion(sample_valid_enriched_df):
    # Test non row flaaged as "Anomaly" is part of the df
    df = sample_valid_enriched_df
    anomalies = df[df["Flag"] == "Anomaly"]

    assert len(anomalies) == 0


def test_pre_write_check(tmp_path):
    # Test if any truncated write process is detected
    base_path = tmp_path / "gold"
    write_path_year = base_path + "/2010"

    # Truncated month (no month folder exist)
    write_path_month = write_path_year + "/10"
    assert save_to_gold.is_dir_non_existent_or_empty(write_path_year, write_path_month)

    # Truncated file (the folder is empty)
    write_path_month = write_path_year + "/11"
    assert save_to_gold.is_dir_non_existent_or_empty(write_path_year, write_path_month)
    """ if os.path.exists(write_path_year):
        
        assert not os.path.exists(write_path_month)

        if os.path.exists(writte_path_month):
            files = list(writte_path_month.glob('*.parquet'))
            assert len(files)==0
        else:
            return True """


def test_save_to_gold_is_idempotent(tmp_path, sample_valid_enriched_df):
    # Test the correct behavior when the process is trigged two times
    base_path = tmp_path / "gold"

    # First execution
    save_to_gold(sample_valid_enriched_df, str(base_path))
    initial_files = list(base_path.rglob("*.parquet"))

    # Second execution (duplicated)
    save_to_gold(sample_valid_enriched_df, str(base_path))
    final_files = list(base_path.rglob("*.parquet"))

    # Assert: There is no extra files or duplicated folders
    assert len(initial_files) == len(final_files)
