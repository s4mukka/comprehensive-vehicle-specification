from pyspark.sql import DataFrame

from datalib.utils import get_active_spark

from .DataSystem import DataSystem, DataSystemManager


@DataSystemManager.register("default")
class DefaultDataSystem(DataSystem):
    def __init__(self, config=[]):
        self.spark = get_active_spark(config)

    def load(self, df: DataFrame, table: str, mode: str = "overwrite", **kwargs):
        data_format = kwargs.get("format", "parquet")
        df.write.format(data_format).save(table)

    def read(self, table: str, mode: str = "full", **kwargs) -> DataFrame:
        data_format = kwargs.get("format", "parquet")
        data_options = dict(kwargs.get("options", []))
        df: DataFrame = self.spark.read.format(data_format).options(**data_options).load(table)
        return df
