import numpy as np
import pandas as pd


# Flag to enrich the dataset
def flag_df(df: pd.DataFrame):
    # Flag the data with for business logic
    df_res = df.copy()
    condition_list = [
        ((df_res["Quantity"] == 0) | (df_res["UnitPrice"] < 0)),
        ((df_res["Quantity"] < 0) & (df_res["UnitPrice"] > 0.0)),
        ((df_res["Quantity"] > 0) & (df_res["UnitPrice"] == 0.0)),
    ]
    choice_list = ["Anomaly", "Refund", "Promotion/Gift"]
    df_res["Flag"] = np.select(condition_list, choice_list, default="Standard")
    return df_res


def convert_date_column(df: pd.DataFrame):
    # Convert the 'date_col' to datetime objects
    try:
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
        return df
    except (ValueError, pd.errors.ParserError) as e:
        # This block will execute because 'invalid-date' cannot be parsed
        print(f"Error occurred: {e}")
        print("Execution stopped due to invalid date format.")


def add_partition_date_columns(df: pd.DataFrame):
    df_with_partion_columns = df.copy()
    df_with_partion_columns = convert_date_column(df_with_partion_columns)
    df_with_partion_columns["year"] = df_with_partion_columns["InvoiceDate"].dt.year
    df_with_partion_columns["month"] = df_with_partion_columns["InvoiceDate"].dt.month
    return df_with_partion_columns
