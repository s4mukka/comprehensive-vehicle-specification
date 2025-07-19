from collections.abc import Callable
from functools import wraps
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from .datasystem import DataSystem, DataSystemManager
from .Extractor import Extractor
from .utils import get_active_spark


class ETLManager:
    _registry: dict[str, Any] = {}
    _spark: SparkSession | None = None

    @classmethod
    def register(cls, name: str, transform_cls: type):
        if name in cls._registry:
            raise ValueError(f"Transform '{name}' is already registered.")
        cls._registry[name] = transform_cls

    @classmethod
    def get(cls, name: str):
        return cls._registry.get(name)

    @classmethod
    def list(cls):
        return list(cls._registry.keys())

    @classmethod
    def run(cls, name: str = "", configs=[], **kwargs):
        if not cls._spark:
            cls._spark = get_active_spark(name, configs)

        transform_cls = cls._registry.get(name)
        if not transform_cls:
            raise ValueError(f"Transform '{name}' not found.")
        instance = transform_cls(**kwargs)
        instance.exec(**kwargs)
        return instance

    @classmethod
    def run_all(cls, name="", configs=[]):
        if not cls._spark:
            cls._spark = get_active_spark(name, configs)
        for name in cls._registry:
            cls.run(name)


def transform(run: bool = False):
    def wrapper(cls):
        name = cls.__name__
        ETLManager.register(name, cls)
        if run:
            ETLManager.run(name)
        return cls

    return wrapper


def extract(table: str = "", system: DataSystem | None = None, mode: str = "full", **kwargs):
    data_format = kwargs.get("format", "default")
    system = DataSystemManager.get(data_format)
    return Extractor(table, system, mode, **kwargs)


def load(
    *,
    table: str = "",
    location: str = "",
    format: str = "iceberg",
    mode: str = "overwrite",
    **kwargs,
):
    def decorator(method: Callable):
        @wraps(method)
        def wrapper(self, *args, **inner_kwargs):
            result = method(self, *args, **inner_kwargs)

            if not isinstance(result, DataFrame):
                raise TypeError("The method decorated with @load must return a DataFrame.")

            table_name = table
            if "table_template" in kwargs:
                ctx = getattr(self, "context", {})
                table_name = kwargs.get("table_template").format(**ctx)

            location_path = location
            if "location_template" in kwargs:
                ctx = getattr(self, "context", {})
                location_path = kwargs.get("location_template").format(**ctx)

            used_system = DataSystemManager.get(format)
            used_system.load(
                df=result,
                table=table_name.lower(),
                mode=mode,
                location=location_path,
                format=format,
                **kwargs,
            )

            return result

        return wrapper

    return decorator
