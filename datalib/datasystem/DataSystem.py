from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class DataSystem(ABC):
    @abstractmethod
    def read(self, table: str, mode: str = "full", **kwargs) -> DataFrame:
        pass

    @abstractmethod
    def load(self, df: DataFrame, table: str, mode: str = "append", **kwargs) -> None:
        pass


class DataSystemManager:
    _registry: dict[str, type[DataSystem]] = {}

    @classmethod
    def register(cls, name: str):
        def decorator(data_system_cls: type[DataSystem]):
            if name in cls._registry:
                raise ValueError(f"DataSystem '{name}' já está registrado.")
            cls._registry[name] = data_system_cls
            return data_system_cls

        return decorator

    @classmethod
    def get(cls, name):
        if not name or name not in cls._registry:
            name = "default"
        return cls._registry.get(name)()

    @classmethod
    def list(cls):
        return list(cls._registry.keys())
