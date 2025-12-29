import pytest 
import pandas as pd
import datetime
from src.ecommerce_etl import source_validator


#Valid data source parameters test
def test_wrong_path_raises_config_error(non_existent_base_path):
    test_year = 2009
    test_month= 1
    # Arrange: A date, where we know that there is no data
    with pytest.raises(FileNotFoundError):
        source_validator.validate_correct_path(base_path=non_existent_base_path, year=test_year, month=test_month)

def test_validate_content_raises_error_when_empty():
    # Arrange: 
    df_empty = pd.DataFrame()
    
    # Act & Assert
    with pytest.raises(source_validator.EmptyDataSourceError):
        source_validator.validate_content_not_empty(df_empty)

# Struture Enforcement test
def test_valid_data_columns():
    #Arrange
    test_columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                        "InvoiceDate", "CustomerID", "Country"]
    df_mismatch = pd.DataFrame(columns=test_columns)
    with pytest.raises(source_validator.SchemaMismatchError):
        source_validator.validate_source_structure(df_mismatch)    

def test_valid_data_type(sample_invalid_df):
    with pytest.raises(source_validator.DataTypesMismatchError):
        source_validator.validate_data_types(sample_invalid_df)
       

#Test de Business errors
def test_transaction_with_negative_quatity_raises_business_error():
    pass

def test_transaction_with_negative_unit_price_raises_business_error(sample_invalid_df):
    with pytest.raises(source_validator.UnitPriceError):
        source_validator.validate_unit_price(sample_invalid_df)