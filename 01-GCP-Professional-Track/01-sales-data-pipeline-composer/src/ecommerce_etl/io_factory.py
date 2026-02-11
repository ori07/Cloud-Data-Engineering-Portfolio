class IOProtocolError(Exception):
    """Custom Exception for clarity in logs."""

    pass


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
        # The protocol is extracted from URI (ej: 's3' de 's3://bucket/...')
        protocol = uri.split("://")[0] if "://" in uri else "file"

        manager_class = cls._registry.get(protocol)
        if not manager_class:
            raise IOProtocolError(
                f"There is no IOManager registered for the protocol: {protocol}"
            )

        return manager_class(**kwargs)
