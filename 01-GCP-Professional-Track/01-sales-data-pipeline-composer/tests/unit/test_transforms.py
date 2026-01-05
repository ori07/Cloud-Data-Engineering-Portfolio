from src.ecommerce_etl import transforms


def test_flagged_df(sample_df_to_flag):
    # Arrange to get a flagged df
    enriched_df = transforms.flag_df(sample_df_to_flag)

    # Assert: Verify secific cases by using Pandas filters
    # Case 1: Quantity 0 -> Anomaly
    assert len(enriched_df[enriched_df["Flag"] == "Anomaly"]) == 2

    # Case 2: Negative quantity and price > 0 -> Refunds
    assert len(enriched_df[enriched_df["Flag"] == "Refund"]) == 1

    # Case 3: Price 0 and quatity > 0 -> Promotion
    assert len(enriched_df[enriched_df["Flag"] == "Promotion/Gift"]) == 1

    # Case 4: Regular sell (Price >0 & Quantity >0) -> Standard
    assert len(enriched_df[enriched_df["Flag"] == "Standard"]) == 1

    # Case non null in the new column
    assert enriched_df["Flag"].isna().sum() == 0


def test_date_column_type(sample_enriched_df):
    # Test if the column with the date in dataframe has te correct type
    df = transforms.convert_date_column(sample_enriched_df)
    actual_type = df["InvoiceDate"].dtype
    expected_type = "datetime64[ns]"

    assert actual_type == expected_type


def test_check_partitions_columns_exist(sample_enriched_df):
    df = transforms.convert_date_column(sample_enriched_df)
    df = transforms.add_partition_date_columns(df)
    assert "year" in list(df.columns) and "month" in list(df.columns)


""" def test_transaction_with_negative_quatity_raises_business_error():
    pass
    qué pasa con precio y cantidad cero?

def test_transaction_with_negative_unit_price_raises_business_error(sample_invalid_df):
    with pytest.raises(source_validator.UnitPriceError):
        source_validator.validate_unit_price(sample_invalid_df) """
