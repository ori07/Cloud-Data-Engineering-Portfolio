import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


class CorruptDataError(Exception):
    """Custom Exception for clarity in logs."""

    pass


def discard_anomalies(df: pd.DataFrame):
    # Drop the anomalies
    filter_condition = df["Flag"] == "Anomaly"
    anomalies = df[filter_condition]
    df_clean = df[~filter_condition]
    return [anomalies, df_clean]


def get_file_list(path):
    files = list(path.glob("*.parquet"))
    return files


def save_to_gold(df, saving_path):
    # Discard the anomalies in data
    anomalies, df_clean = discard_anomalies(df)
    if df_clean.empty:
        raise CorruptDataError("No valid data to save after discarding anomalies")

    # Set up the filesystem
    # Prepare the dataset

    # Convert pandas DataFrame to an Arrow Table
    table = pa.Table.from_pandas(df_clean)
    # Write the dataset with Hive partitioning
    ds.write_dataset(
        data=table,
        base_dir=saving_path,
        format="parquet",
        partitioning=["year", "month"],  # Columns to partition by
        partitioning_flavor="hive",  # Use Hive-style partitioning
        existing_data_behavior="overwrite_or_ignore",  # Handle existing data
    )
