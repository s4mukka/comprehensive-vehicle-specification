import pyspark.sql.functions as F
import pyspark.sql.types as T

from datalib.etl import ETLManager, extract, load, transform


@transform()
class DataValidate:
    df = extract(table="raw.vehicles.cars", format="iceberg")

    def _extract_value(self, column_name):
        return F.regexp_extract(F.col(column_name), r"(\d+\.?\d*)", 1).cast(T.FloatType())

    @load(format="iceberg", mode="overwrite", table="trusted.vehicles.cars", keys=["name"])
    def exec(self, **kwargs):
        return self.df.dropDuplicates(["name"]).withColumns(
            {
                "mileage": self._extract_value("mileage"),
                "cc": self._extract_value("cc"),
                "power": self._extract_value("power"),
                "_processing_timestamp": F.current_timestamp(),
            }
        )


if __name__ == "__main__":
    ETLManager.run_all(name="TrustedCars")
