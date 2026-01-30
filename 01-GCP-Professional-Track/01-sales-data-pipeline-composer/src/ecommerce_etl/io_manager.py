class IOFactory:
    _registry = {}

    @classmethod
    def register(cls, protocol: str):
        def wrapper(subclass):
            cls._registry[protocol] = subclass
            return subclass

        return wrapper

    @classmethod
    def get_manager(cls, uri: str, **kwargs):
        # Extraemos el protocolo del URI (ej: 's3' de 's3://bucket/...')
        protocol = uri.split("://")[0] if "://" in uri else "file"

        manager_class = cls._registry.get(protocol)
        if not manager_class:
            raise ValueError(
                f"There is no IOManager registered for the protocol: {protocol}"
            )

        return manager_class(**kwargs)


# # Complementary functions
# def get_source_path(base_path: str, execution_date: datetime = None) -> str:
#     if execution_date is None:
#         execution_date = datetime.now()

#     year = execution_date.year
#     month = execution_date.month
#     # canonical path
#     # Set up the filesystem
#     filesystem, path = pa.fs.FileSystem.from_uri(base_path)
#     execution_path = base_path + f"/{year}/{month}/sales_{year}_{month}.csv"
#     return execution_path
#
# # Validation functions
# def validate_correct_path(base_path: str, year: int = None, month: int = None):
#     if year is None or month is None:
#         date_searched = None
#     else:
#         date_searched = datetime(year=year, month=month, day=1)
#     execut_path = get_source_path(base_path=base_path, execution_date=date_searched)
#     # Handle file no found error
#     if not os.path.exists(execution_path):
#         raise FileNotFoundError
#     return execution_path
