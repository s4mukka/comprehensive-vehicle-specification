from pyspark.sql import SparkSession


def get_active_spark(name="", configs=[]) -> SparkSession | None:
    spark = None
    spark = SparkSession.getActiveSession()
    if not spark:
        spark_builder = SparkSession.builder.appName(name)
        for config in configs:
            spark_builder = spark_builder.config(*config)
        spark = spark_builder.getOrCreate()

    return spark
