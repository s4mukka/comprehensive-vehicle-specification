import pyspark.sql.functions as F

from datalib.etl import ETLManager, extract, load, transform


@transform()
class DataValidate:
    df = extract(table="/app/data/landing/car/", format="json", options=(("multiline", "true"),))

    @load(format="iceberg", mode="overwrite", table="raw.vehicles.cars", keys=["name"])
    def exec(self, **kwargs):
        return (
            self.df.withColumn(
                "_validation_details",
                F.concat_ws(
                    ", ",
                    F.when(F.col("type").isNull() | (F.col("type") == ""), "type_is_missing"),
                    F.when(F.col("name").isNull() | (F.col("name") == ""), "name_is_missing"),
                ),
            )
            .withColumn(
                "_validation_status",
                F.when(F.col("_validation_details") == "", "VALID").otherwise("INVALID"),
            )
            .withColumn("_processing_timestamp", F.current_timestamp())
        )


if __name__ == "__main__":
    ETLManager.run_all(name="RawCars")
