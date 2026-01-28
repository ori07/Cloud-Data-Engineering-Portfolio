import pandera.polars as pa
import polars as pl
import pytest

from src.ecommerce_etl import source_validator


# Valid data source parameters test
@pytest.mark.skip(reason="This feature is currently under refactor")
def test_wrong_path_raises_config_error(non_existent_base_path):
    test_year = 2009
    test_month = 1
    # Arrange: A date, where we know that there is no data
    with pytest.raises(FileNotFoundError):
        source_validator.validate_correct_path(
            base_path=non_existent_base_path, year=test_year, month=test_month
        )


def test_schema_strictness_missing_column(sample_valid_df):
    # Arrange: DF with misses "UnitPrice" column
    df_missing_col = sample_valid_df.drop("UnitPrice")

    # Act & Assert
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        source_validator.InvoiceSchema.validate(df_missing_col)


def test_schema_strictness_extra_column():
    # Arrange: Agregamos una columna que no existe en el esquema
    df_extra = pl.DataFrame(
        {
            "InvoiceNo": [1],
            "StockCode": ["A"],
            "Description": ["Ok"],
            "Quantity": [1],
            "InvoiceDate": ["2026-01-01"],
            "UnitPrice": [10.5],
            "CustomerID": [123],
            "Country": ["Chile"],
            "Columna_Invasora": ["Error"],
        }
    )

    # Act & Assert
    with pytest.raises(
        pa.errors.SchemaError, match="column 'Columna_Invasora' not in DataFrameSchema"
    ):
        source_validator.InvoiceSchema.validate(df_extra)


# @pytest.mark.skip(reason="This feature is currently under development")
def test_validate_content_raises_error_when_empty():
    # Arrange:
    df_empty = pl.DataFrame(
        data=[],
        schema={
            "InvoiceNo": pl.Int64,
            "StockCode": pl.String,
            "Description": pl.String,
            "Quantity": pl.Int64,
            "InvoiceDate": pl.Datetime("us"),
            "UnitPrice": pl.Float64,
            "CustomerID": pl.Int64,
            "Country": pl.String,
        },
    )
    df_empty = df_empty.slice(0, 0)

    # Act & Assert
    with pytest.raises(source_validator.EmptyDataSourceError):
        source_validator.validate_data_source(df_empty)


# Wrong Data types
def test_valid_data_type(sample_invalid_df):
    with pytest.raises(pa.errors.SchemaError) as exc_info:
        source_validator.InvoiceSchema.validate(sample_invalid_df)
    assert "Column 'UnitPrice' failed validator number" in str(exc_info.value)


# Valid DF
def test_valid_schema_passes(sample_valid_df):
    assert source_validator.InvoiceSchema.validate(sample_valid_df) is not None
