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

@pytest.fixture
def sample_df_to_flag():
    """
    Provides a DataFrame with the correct case to test.
    """
    import pandas as pd
    return pd.DataFrame({
        "InvoiceNo": [536365,536366, 536367, 536368, 536369 ],
        "StockCode": ["85123A", "85123B", "85123C", "85123D", "85123D"],
        "Description": ["WHITE HANGING HEART T-LIGHT HOLDER", "Perfum Vegetal", "T-shirt B", "C", "D"],
        "Quantity": [6, -2, 0,2,8],
        "InvoiceDate": ["2010-12-01 08:26:00","2010-12-01 08:26:00","2010-12-01 08:26:00","2010-12-01 08:26:00", "2010-12-01 08:26:00"],
        "UnitPrice": [7.00, 3.5, 5.2,0.0,-2.5],
        "CustomerID": [17850,17850,17851, 17852, 17852],
        "Country": ["United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom"]
    })


