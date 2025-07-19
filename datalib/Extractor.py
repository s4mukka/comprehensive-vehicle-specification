from functools import cache

from .datasystem import DataSystem


@cache
class Extractor:
    def __init__(
        self, table: str = "", system: DataSystem | None = None, mode: str = "full", **kwargs
    ):
        self.table = table
        self.system = system
        self.name = None
        self.kwargs = kwargs

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if self.name not in instance.__dict__:
            table_name = self.table
            instance.__dict__[self.name] = self.system.read(table=table_name.lower(), **self.kwargs)
        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        instance.__dict__[self.name] = value
