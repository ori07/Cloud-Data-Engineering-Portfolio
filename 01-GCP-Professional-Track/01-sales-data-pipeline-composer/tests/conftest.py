# conftest.py
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.ecommerce_etl.data_source import CSVDataSource


@pytest.fixture
def mock_env():
    """Limpia el entorno antes y después de cada test."""
    old_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(old_env)


@pytest.fixture
def csv_datasource_instance():
    """Return a CSVDataSource instance."""
    return CSVDataSource()


@pytest.fixture
def non_existent_base_path():
    """
    Provides a route which does not exist
    """
    return "/tmp/data/this_path_is_fake_12345"


@pytest.fixture
def mock_fsspec():
    with patch("fsspec.filesystem") as mock_factory:
        mock_fs = MagicMock()
        mock_factory.return_value = mock_fs
        yield mock_fs  # El test recibe el mock_fs directamente


@pytest.fixture
def sample_valid_df():
    """
    Provides a DataFrame with the correct structure for test of 'Happy Path'.
    """
    return pl.DataFrame(
        {
            "InvoiceNo": ["536365"],
            "StockCode": ["85123A"],
            "Description": ["WHITE HANGING HEART T-LIGHT HOLDER"],
            "Quantity": [6],
            "InvoiceDate": [datetime.now()],
            "UnitPrice": [2.55],
            "CustomerID": ["17850"],
            "Country": ["United Kingdom"],
        }
    )


@pytest.fixture
def sample_invalid_df():
    """
    Provides a DataFrame with the correct structure but incorrect datatypes.
    """
    return pl.DataFrame(
        {
            "InvoiceNo": [536365],
            "StockCode": ["85123A"],
            "Description": ["WHITE HANGING HEART T-LIGHT HOLDER"],
            "Quantity": [6.0],
            "InvoiceDate": [datetime.now()],
            "UnitPrice": [-2.55],
            "CustomerID": [17850],
            "Country": ["United Kingdom"],
        }
    )


@pytest.fixture
def sample_df_to_flag():
    """
    Provides a Polars DataFrame with scenarios for all business flags.
    """
    import polars as pl

    return pl.DataFrame(
        {
            "InvoiceNo": [536365, 536366, 536367, 536368, 536369],
            "StockCode": ["85123A", "85123B", "85123C", "85123D", "85123E"],
            "Description": [
                "Standard",
                "Refund",
                "Anomaly Qty",
                "Gift",
                "Anomaly Price",
            ],
            "Quantity": [6, -2, 0, 2, 8],
            "InvoiceDate": ["2010-12-01 08:26:00"] * 5,
            "UnitPrice": [7.00, 3.5, 5.2, 0.0, -2.5],
            "CustomerID": [17850, 17850, 17851, 17852, 17853],
            "Country": ["United Kingdom"] * 5,
        },
        schema={
            "InvoiceNo": pl.Int64,
            "StockCode": pl.String,
            "Description": pl.String,
            "Quantity": pl.Int64,
            "InvoiceDate": pl.String,
            "UnitPrice": pl.Float64,
            "CustomerID": pl.Int64,
            "Country": pl.String,
        },
    )


@pytest.fixture
def sample_enriched_df():
    """
    Represents the state AFTER flag_df has run.
    Contains 5 rows (including 2 anomalies).
    """
    return pl.DataFrame(
        {
            "InvoiceNo": [536365, 536366, 536367, 536368, 536369],
            "StockCode": ["85123A", "85123B", "85123C", "85123D", "85123D"],
            "Description": ["WHITE...", "Perfum...", "T-shirt...", "C", "D"],
            "Quantity": [6, -2, 0, 2, 8],
            "InvoiceDate": ["2010-12-01 08:26:00"] * 5,
            "UnitPrice": [7.00, 3.5, 5.2, 0.0, -2.5],
            "CustomerID": [17850, 17850, 17851, 17852, 17852],
            "Country": ["United Kingdom"] * 5,
            "Flag": ["Standard", "Refund", "Anomaly", "Promotion/Gift", "Anomaly"],
        }
    )


@pytest.fixture
def sample_valid_enriched_df():
    """
    Represents the expected result AFTER discard_anomalies.
    Should contain only the 3 non-anomaly rows.
    """
    return pl.DataFrame(
        {
            "InvoiceNo": [536365, 536366, 536368],
            "StockCode": ["85123A", "85123B", "85123D"],
            "Description": ["WHITE...", "Perfum...", "T-shirt B"],
            "Quantity": [6, -2, 2],
            "InvoiceDate": ["2010-12-01 08:26:00"] * 3,
            "UnitPrice": [7.00, 3.5, 0.0],
            "CustomerID": [17850, 17850, 17852],
            "Country": ["United Kingdom"] * 3,
            "Flag": ["Standard", "Refund", "Promotion/Gift"],
        }
    )


@pytest.fixture
def sample_valid_partitionable_df():
    """
    Provides a DataFrame with the correct case to test.
    """
    return pl.DataFrame(
        {
            "InvoiceNo": [536365, 536366, 536368],
            "StockCode": ["85123A", "85123B", "85123D"],
            "Description": [
                "WHITE HANGING HEART T-LIGHT HOLDER",
                "Perfum Vegetal",
                "T-shirt B",
            ],
            "Quantity": [6, -2, 2],
            "InvoiceDate": [
                "2010-12-01 08:26:00",
                "2010-12-01 08:26:00",
                "2010-11-01 08:26:00",
            ],
            "UnitPrice": [7.00, 3.5, 0.0],
            "CustomerID": [17850, 17850, 17852],
            "Country": [
                "United Kingdom",
                "United Kingdom",
                "United Kingdom",
            ],
            "Flag": ["Standard", "Refund", "Promotion/Gift"],
            "year": [2010, 2010, 2010],
            "month": [12, 12, 11],
        },
    )
