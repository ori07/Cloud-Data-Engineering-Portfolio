from abc import ABC, abstractmethod

import polars as pl

from .data_source_factory import DataSourceFactory


class DataSource(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def read(self, stream):
        pass


@DataSourceFactory.register(".csv")
class CSVDataSource(DataSource):
    def __init__(self):
        super().__init__()

    def read(self, stream):
        df = pl.read_csv(stream)
        return df


@DataSourceFactory.register(".parquet")
class ParquetDataSource(DataSource):
    def __init__(self):
        super().__init__()

    def read(self, stream):
        df = pl.read_parquet(stream)
        return df
