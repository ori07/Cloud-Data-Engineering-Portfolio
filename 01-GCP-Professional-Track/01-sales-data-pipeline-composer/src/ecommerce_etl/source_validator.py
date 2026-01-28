from datetime import datetime

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

"""
    A class that validate that the raw data:
    1. Exist (there is no empty file or incorrect path)
    2. The data conforms to the expected structure.
    3. The data types conform to the expected types.
"""


class EmptyDataSourceError(Exception):
    """Custom Exception for clarity in logs."""


class InvoiceSchema(pa.DataFrameModel):
    InvoiceNo: Series[int] = pa.Field(nullable=False, ge=0)
    StockCode: Series[str] = pa.Field(nullable=False)
    Description: Series[str]
    Quantity: Series[int] = pa.Field(nullable=False)
    InvoiceDate: Series[pl.Datetime]
    UnitPrice: Series[float] = pa.Field(ge=1)
    CustomerID: Series[int] = pa.Field(nullable=False, ge=0)
    Country: Series[str]

    class Config:
        # Coerce types if necessary
        coerce = True
        strict = True


def validate_data_source(df: pl.DataFrame):
    if df.is_empty():
        raise EmptyDataSourceError("Files without data.")

    try:
        # Intentamos validar el DataFrame
        df_clean = InvoiceSchema.validate(df, lazy=True)
        print("✅ ¡Successfull Validation! The data are clean.")
        return df_clean

    except pa.errors.SchemaErrors as err:
        # 1. Get the failure report
        failure_report = err.failure_cases

        # 2. We save the failed rows
        now = datetime.now().strftime("%Y%m%d_%H%M")
        # # TODO: define where to save it, get the current file system
        failure_report.to_csv(f"reports/err_{now}.csv")

        # 3. Filtering and follow with the correct data
        invalid_indices = err.failure_cases["index"].unique()
        df_clean = df.drop(index=invalid_indices)
        print(f"Se descartaron {len(invalid_indices)} filas corruptas.")
        return df_clean
