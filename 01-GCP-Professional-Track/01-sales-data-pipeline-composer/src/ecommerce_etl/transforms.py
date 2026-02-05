from typing import List

import polars as pl


# Flag to enrich the dataset
def flag_df(df: pl.DataFrame) -> pl.DataFrame:
    # Flag the data with for business logic
    return df.with_columns(
        pl.when((pl.col("Quantity") == 0) | (pl.col("UnitPrice") < 0))
        .then(pl.lit("Anomaly"))
        .when((pl.col("Quantity") < 0) & (pl.col("UnitPrice") > 0.0))
        .then(pl.lit("Refund"))
        .when((pl.col("Quantity") > 0) & (pl.col("UnitPrice") == 0.0))
        .then(pl.lit("Promotion/Gift"))
        .otherwise(pl.lit("Standard"))
        .alias("Flag")
    )


# def convert_date_column(df: pl.DataFrame):
#     # Convert the 'date_col' to datetime objects
#     try:
#         pl.col("InvoiceDate").str.to_date("%Y-%m-%d")
#         return df
#     except (ValueError, pl.exceptions.InvalidOperationError) as e:
#         # This block will execute because 'invalid-date' cannot be parsed
#         print(f"Error occurred: {e}")
#         print("Execution stopped due to invalid date format.")


# def add_partition_columns(df: pl.DataFrame) -> pl.DataFrame:
#     return df.with_columns([
#         pl.col("InvoiceDate").dt.year().alias("year"),
#         pl.col("InvoiceDate").dt.month().alias("month")
#     ])

# def enrich_data(df_validated):
#     df_enriched = flag_df(df_validated)
#     return df_enriched


# def prepare_partitions(df_validated):
#     df_transformed = convert_date_column(df_validated)
#     df_transformed = add_partition_date_columns(df_transformed)
#     return df_transformed


def discard_anomalies(df: pl.DataFrame) -> List[pl.DataFrame]:
    """
    Splits the DataFrame into two: anomalies and clean data.
    """
    anomalies = df.filter(pl.col("Flag") == "Anomaly")
    df_clean = df.filter(pl.col("Flag") != "Anomaly")

    return [anomalies, df_clean]
