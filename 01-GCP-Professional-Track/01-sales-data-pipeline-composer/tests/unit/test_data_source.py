import pytest 
import pandas as pd
import datetime
from src.ecommerce_etl import source_validator
from src.ecommerce_etl import EmptyDataSourceError

#Test 

# Struture Enforcement test
def test_valid_data_columns():

    expected_columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                        "InvoiceDate", "UnitPrice", "CustomerID", "Country"]    
    #Get dataframe
    df = source_validator.get_data_source()
    actual_columns = df.columns
    assert list(actual_columns) == expected_columns


def test_valid_data_type():
    df = source_validator.get_data_source()
    # Assert
    assert df["Quantity"].dtype == "int64"
    assert df["UnitPrice"].dtype == "float64"
    assert df["InvoiceNo"].dtype == "object"


def test_empty_data_source():
    # Arrange
    path_empty_file = "data/testing/empty_file.csv"
    
    # Act & Assert
    with pytest.raises(EmptyDataSourceError):
        source_validator.get_data_source(path_empty_file)

def test_wrong_path_raises_config_error():
    # Arrange: A date, where we know that there is no data
    future_date = datetime.datetime(2030, 1, 1)
    #Handle file no found error
    with pytest.raises(FileNotFoundError):
        source_validator.get_data_source(execution_date=future_date)


#Test de Business errors
def test_transaction_with_negative_quatity_raises_business_error():
    pass

def test_transaction_with_negative_unit_price_raises_business_error():
    pass



