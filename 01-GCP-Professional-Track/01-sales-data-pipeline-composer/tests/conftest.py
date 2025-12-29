# conftest.py
import pytest
from pathlib import Path

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
    import pandas as pd
    return pd.DataFrame({
        "InvoiceNo": ["536365"],
        "StockCode": ["85123A"],
        "Description": ["WHITE HANGING HEART T-LIGHT HOLDER"],
        "Quantity": [6],
        "InvoiceDate": ["2010-12-01 08:26:00"],
        "UnitPrice": [2.55],
        "CustomerID": ["17850"],
        "Country": ["United Kingdom"]
    })

@pytest.fixture
def sample_invalid_df():
    """
    Provides a DataFrame with the correct structure for test of 'Happy Path'.
    """
    import pandas as pd
    return pd.DataFrame({
        "InvoiceNo": [536365],
        "StockCode": ["85123A"],
        "Description": ["WHITE HANGING HEART T-LIGHT HOLDER"],
        "Quantity": [6.0],
        "InvoiceDate": ["2010-12-01 08:26:00"],
        "UnitPrice": [-2.55],
        "CustomerID": [17850],
        "Country": ["United Kingdom"]
    })

