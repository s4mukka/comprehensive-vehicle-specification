from datalib.etl import ETLManager, extract, load, transform


@transform()
class DataValidate:
    bikes_df = extract(table="trusted.vehicles.bikes", format="iceberg")
    cars_df = extract(table="trusted.vehicles.cars", format="iceberg")

    @load(format="iceberg", mode="overwrite", table="refined.analytics.vehicles", keys=["name"])
    def exec(self, **kwargs):
        return self.bikes_df.unionByName(self.cars_df, allowMissingColumns=True)


if __name__ == "__main__":
    ETLManager.run_all(name="RefinedAnalyticsVehicles")
