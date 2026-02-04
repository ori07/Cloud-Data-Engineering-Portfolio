from abc import ABC, abstractmethod

import fsspec as fs
import pyarrow.dataset as ds

from .io_factory import IOFactory


class IOManagerError(Exception):
    pass


class IOManagerPermissionsError(Exception):
    pass


class IOManager(ABC):
    """Implements the interface for every protocol to connect to
    the infraestruture layer for load and save data
    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass

    @abstractmethod
    def open_stream(self, path: str):
        pass

    @abstractmethod
    def save_dataframe(self, df, path: str):
        pass

    @abstractmethod
    def get_file_list(self, path: str):
        pass


@IOFactory.register("gs")
class CloudIOManager(IOManager):
    def __init__(self, **bucket_config):
        super().__init__()
        self.fs = fs.filesystem("gs", **bucket_config)

    def exists(self, path):
        try:
            return self.fs.exists(path)
        except Exception as e:
            raise IOManagerError(f"File not found at {path}: {str(e)}")

    def open_stream(self, path):
        try:
            return self.fs.open(path, mode="rb")
        except Exception as e:
            raise IOManagerPermissionsError(
                f"Pemission denied to read file {path}: {str(e)}"
            )

    def get_file_list(self, path):
        files = self.fs.find(path)  # list recursively all the files
        return files

    def save_dataframe(self, df, path):
        # Prepare the dataset
        # Convert pandas DataFrame to an Arrow Table
        table = df.to_arrow()
        # Write the dataset with Hive partitioning
        ds.write_dataset(
            data=table,
            base_dir=path,
            format="parquet",
            partitioning=["year", "month"],  # Columns to partition by
            partitioning_flavor="hive",  # Use Hive-style partitioning
            filesystem=self.fs,
            existing_data_behavior="delete_matching",  # Handle existing data
        )


@IOFactory.register("file")
class LocalIOManager(IOManager):
    def __init__(self, **kwargs):
        super().__init__()
        # Creamos un handler local explícito para evitar errores en save_dataframe
        self.fs = fs.filesystem("file")

    def exists(self, path):
        try:
            return self.fs.exists(path)
        except Exception as e:
            raise IOManagerError(f"File not found at {path}: {str(e)}")

    def open_stream(self, path):
        return self.fs.open(path, mode="rb")

    def get_file_list(self, path):
        return self.fs.find(path)

    def save_dataframe(self, df, path):
        # Prepare the dataset
        # Convert pandas DataFrame to an Arrow Table
        table = df.to_arrow()
        # Write the dataset with Hive partitioning
        ds.write_dataset(
            data=table,
            base_dir=path,
            format="parquet",
            partitioning=["year", "month"],  # Columns to partition by
            partitioning_flavor="hive",  # Use Hive-style partitioning
            filesystem=self.fs,
            existing_data_behavior="delete_matching",  # Handle existing data
        )
