import polars as pl
import pytest

from src.ecommerce_etl import transforms


def test_flagged_df(sample_df_to_flag):
    # Arrange to get a flagged df
    enriched_df = transforms.flag_df(sample_df_to_flag)

    # Assert: Using Polars native filtering and len()
    # Case 1: Quantity 0 -> Anomaly
    assert enriched_df.filter(pl.col("Flag") == "Anomaly").height == 2

    # Case 2: Negative quantity and price > 0 -> Refunds
    assert enriched_df.filter(pl.col("Flag") == "Refund").height == 1

    # Case 3: Price 0 and quatity > 0 -> Promotion
    assert enriched_df.filter(pl.col("Flag") == "Promotion/Gift").height == 1

    # Case 4: Regular sell (Price >0 & Quantity >0) -> Standard
    assert enriched_df.filter(pl.col("Flag") == "Standard").height == 1

    # Case non null in the new column
    assert enriched_df["Flag"].null_count() == 0


@pytest.mark.skip(reason="This feature is currently under refactor")
def test_date_column_type(sample_enriched_df):
    # Test if the column with the date in dataframe has te correct type
    df = transforms.convert_date_column(sample_enriched_df)
    actual_type = df["InvoiceDate"].dtype
    expected_type = "datetime64[ns]"

    assert actual_type == expected_type


@pytest.mark.skip(reason="This feature is currently under refactor")
def test_check_partitions_columns_exist(sample_enriched_df):
    df = transforms.convert_date_column(sample_enriched_df)
    df = transforms.add_partition_date_columns(df)
    assert "year" in list(df.columns) and "month" in list(df.columns)


# @pytest.mark.skip(reason="This feature is currently under refactor")
def test_anomaly_exclusion(sample_valid_enriched_df):
    # Test non row flaaged as "Anomaly" is part of the df
    df = sample_valid_enriched_df
    anomalies = transforms.discard_anomalies(df)[0]
    assert len(anomalies) == 0
