import os
import pandas as pd
from datetime import datetime


class EmptyDataSourceError(Exception):
    """Custom Exception for clarity in logs."""
    pass

class SchemaMismatchError(Exception): 
    """Custom Exception for clarity in logs."""
    pass

class DataTypesMismatchError(Exception):
    """Custom Exception for clarity in logs."""
    pass

class UnitPriceError(Exception):
    """Custom Exception for clarity in logs."""
    pass

# Complementary functions
def get_source_path(base_path: str, execution_date: datetime = None) -> str:
    if execution_date is None:
        execution_date = datetime.now()
    
    year = execution_date.year
    month = execution_date.month
    # canonical path
    execution_path = base_path + f"/{year}/{month}/sales_{year}_{month}.csv"
    return execution_path


def get_data_source(path, separator):
    df = pd.read_csv(path, sep=separator) 
    return df

# Validation functions
def validate_correct_path(base_path: str, year: int=None, month: int=None):
    if year is None or month is None:
        date_searched = None
    else:
        date_searched = datetime(year=year,month=month, day=1)
    execution_path = get_source_path(base_path=base_path, execution_date=date_searched)

    #Handle file no found error
    if not os.path.exists(execution_path):
        raise FileNotFoundError
    return execution_path

def validate_content_not_empty(df: pd.DataFrame):
    if df.empty:
        raise EmptyDataSourceError(f"The files is empty. Aborting pipeline.")
    return df

def validate_source_structure(df: pd.DataFrame):
    expected_columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                        "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    if list(df.columns) != expected_columns:
        raise SchemaMismatchError("The file's columns do not match with expected schema.")
    return True

def validate_data_types(df: pd.DataFrame):
    checks = {
        "InvoiceNo": "object",
        "StockCode": "object",
        "Description": "object",
        "Quantity": "int64",
        "InvoiceDate": "date",
        "UnitPrice": "float64",
        "CustomerID": "object", 
        "Country": "object"
    }
    
    for col, expected_dtype in checks.items():
        if df[col].dtype != expected_dtype:
            raise DataTypesMismatchError(
                f"Column '{col}' expected {expected_dtype} but got {df[col].dtype}"
            )
    return True






    

