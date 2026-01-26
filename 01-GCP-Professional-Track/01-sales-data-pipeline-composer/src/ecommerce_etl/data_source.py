from abc import ABC, abstractmethod

import fsspec as fs
import polars as pl

from .factory import DataSourceFactory


class DataSource(ABC):
    def __init__(self, origin):
        super().__init__()
        self.set_origin(origin)

    def set_origin(self, origin):
        fs_instance, path = fs.core.url_to_fs(origin)
        self.path = path
        self.fs_instance = fs_instance

    @abstractmethod
    def read():
        pass


@DataSourceFactory.register(".csv")
class CSVDataSource(DataSource):
    def __init__(self, origin):
        super().__init__(origin)

    def read(self):
        df = pl.read_csv(self.path)
        return df


@DataSourceFactory.register("Develop-local")
class LocalMockDataSource(DataSource):
    def __init__(self, origin):
        super().__init__(origin)

    def read(self):
        df = pl.read_csv(self.path)
        return df
