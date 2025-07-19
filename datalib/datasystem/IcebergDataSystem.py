from pyspark.sql import DataFrame

from datalib.utils import get_active_spark

from .DataSystem import DataSystem, DataSystemManager


@DataSystemManager.register("iceberg")
class IcebergDataSystem(DataSystem):
    def __init__(self, config=[]):
        self.spark = get_active_spark(config)
        self._data_system = "iceberg"
        self._write_modes = {
            "overwrite": self._overwrite,
            "append": self._append,
            "merge": self._merge,
        }

    def load(self, df: DataFrame, table: str, mode: str = "overwrite", **kwargs):
        write_method = self._write_modes.get(mode.lower())
        write_method(df, table, **kwargs)

    def read(self, table: str, mode: str = "full", **kwargs) -> DataFrame:
        df: DataFrame = self.spark.read.format("iceberg").load(table)
        return df

    def _get_catalog_and_table(self, qualified_table_name: str):
        """Helper to split 'catalog.schema.table' into parts."""
        parts = qualified_table_name.split(".")
        catalog_name = parts[0]
        schema_and_table = ".".join(parts[1:])
        return catalog_name, schema_and_table

    def _overwrite(self, df: DataFrame, table: str, **kwargs):
        catalog_name, schema_and_table = self._get_catalog_and_table(table)
        self.spark.sql(f"USE {catalog_name}")
        namespace_parts = schema_and_table.split(".")[:-1]
        if namespace_parts:
            namespace = ".".join(namespace_parts)
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace};")
        df.writeTo(schema_and_table).createOrReplace()

    def _append(self, df: DataFrame, table: str, **kwargs):
        df.writeTo(table).using(self._data_system).append()

    def _merge(self, df: DataFrame, table: str, **kwargs):
        keys: list[str] = kwargs.get("keys", [])

        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in keys])

        df.createOrReplaceTempView("source_updates")

        merge_query = f"""
          MERGE INTO {table} AS target
          USING source_updates AS source
          ON {merge_condition}
          WHEN MATCHED THEN
              UPDATE SET *
          WHEN NOT MATCHED THEN
              INSERT *
        """
        self.spark.sql(merge_query)
        self.spark.catalog.dropTempView("source_updates")
