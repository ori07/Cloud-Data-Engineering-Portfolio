# Dynamic Registration Logic (Open/Closed Principle)


class DataSourceTypeError(Exception):
    """Custom Exception for clarity in logs."""

    pass


class DataSourceFactory:
    _registry = {}

    @classmethod
    def register(cls, key: str):
        """Decorator for datasource classes"""

        def wrapper(datasource_class):
            cls._registry[key] = datasource_class
            return datasource_class

        return wrapper

    @classmethod
    def get_data_source(cls, path: str, mode: str):
        if mode not in cls._registry:
            raise DataSourceTypeError(
                f"Type '{mode}' not supported. Aborting pipeline."
            )

        instance = cls._registry[mode](path)
        return instance
