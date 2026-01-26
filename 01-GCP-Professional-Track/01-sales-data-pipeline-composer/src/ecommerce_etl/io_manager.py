# Implements the load and save data

import fsspec as fs
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs


class CorruptDataError(Exception):
    """Custom Exception for clarity in logs."""

    pass


def _get_file_system(path):
    fs_intance, _ = fs.core.url_to_fs(path)
    return fs_intance


def get_file_list(path):
    files = list(path.glob("*.parquet"))
    return files


def list_gcs_files(path):
    fs_intance = _get_file_system(path)
    files = fs_intance.find(path)  # list recursively all the files
    return files


def save_to_gold(df, saving_path):
    # Set up the filesystem
    filesystem, path = pa.fs.FileSystem.from_uri(saving_path)

    # Prepare the dataset
    # Convert pandas DataFrame to an Arrow Table
    table = pa.Table.from_pandas(df)
    # Write the dataset with Hive partitioning
    ds.write_dataset(
        data=table,
        base_dir=path,
        format="parquet",
        partitioning=["year", "month"],  # Columns to partition by
        partitioning_flavor="hive",  # Use Hive-style partitioning
        filesystem=filesystem,
        existing_data_behavior="delete_matching",  # Handle existing data
    )
