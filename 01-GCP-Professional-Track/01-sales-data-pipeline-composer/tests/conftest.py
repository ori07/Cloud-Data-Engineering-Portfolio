# conftest.py
import os

import pytest

from src.ecommerce_etl.data_source import CSVDataSource, LocalMockDataSource


@pytest.fixture
def mock_env():
    """Limpia el entorno antes y después de cada test."""
    old_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(old_env)


@pytest.fixture
def local_mock_instance():
    """Return a LocalMock instance."""
    return LocalMockDataSource()


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
def sample_valid_df():
    """
    Provides a DataFrame with the correct structure for test of 'Happy Path'.
    """
    from datetime import datetime

    import polars as pl

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
    from datetime import datetime

    import polars as pl

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
    Provides a DataFrame with the correct case to test.
    """
    import pandas as pd

    return pd.DataFrame(
        {
            "InvoiceNo": [536365, 536366, 536367, 536368, 536369],
            "StockCode": ["85123A", "85123B", "85123C", "85123D", "85123D"],
            "Description": [
                "WHITE HANGING HEART T-LIGHT HOLDER",
                "Perfum Vegetal",
                "T-shirt B",
                "C",
                "D",
            ],
            "Quantity": [6, -2, 0, 2, 8],
            "InvoiceDate": [
                "2010-12-01 08:26:00",
                "2010-12-01 08:26:00",
                "2010-12-01 08:26:00",
                "2010-12-01 08:26:00",
                "2010-12-01 08:26:00",
            ],
            "UnitPrice": [7.00, 3.5, 5.2, 0.0, -2.5],
            "CustomerID": [17850, 17850, 17851, 17852, 17852],
            "Country": [
                "United Kingdom",
                "United Kingdom",
                "United Kingdom",
                "United Kingdom",
                "United Kingdom",
            ],
        }
    )


@pytest.fixture
def sample_enriched_df():
    """
    Provides a DataFrame with the correct case to test.
    """
    import pandas as pd

    return pd.DataFrame(
        {
            "InvoiceNo": [536365, 536366, 536367, 536368, 536369],
            "StockCode": ["85123A", "85123B", "85123C", "85123D", "85123D"],
            "Description": [
                "WHITE HANGING HEART T-LIGHT HOLDER",
                "Perfum Vegetal",
                "T-shirt B",
                "C",
                "D",
            ],
            "Quantity": [6, -2, 0, 2, 8],
            "InvoiceDate": [
                "2010-12-01 08:26:00",
                "2010-12-01 08:26:00",
                "2010-12-01 08:26:00",
                "2010-12-01 08:26:00",
                "2010-12-01 08:26:00",
            ],
            "UnitPrice": [7.00, 3.5, 5.2, 0.0, -2.5],
            "CustomerID": [17850, 17850, 17851, 17852, 17852],
            "Country": [
                "United Kingdom",
                "United Kingdom",
                "United Kingdom",
                "United Kingdom",
                "United Kingdom",
            ],
            "Flag": ["Standard", "Refund", "Anomaly", "Promotion/Gift", "Anomaly"],
        },
    )


@pytest.fixture
def sample_valid_enriched_df():
    """
    Provides a DataFrame with the correct case to test.
    """
    import pandas as pd

    return pd.DataFrame(
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
                "2010-12-01 08:26:00",
            ],
            "UnitPrice": [7.00, 3.5, 0.0],
            "CustomerID": [17850, 17850, 17852],
            "Country": [
                "United Kingdom",
                "United Kingdom",
                "United Kingdom",
            ],
            "Flag": ["Standard", "Refund", "Promotion/Gift"],
        },
    )


@pytest.fixture
def sample_valid_partitionable_df():
    """
    Provides a DataFrame with the correct case to test.
    """
    import pandas as pd

    return pd.DataFrame(
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
