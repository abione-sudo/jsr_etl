from pyspark.sql import SparkSession


def get_spark_session(app_Name='JSR'):
    spark=SparkSession.builder.appName(app_Name) \
                         .config('spark.driver.memory','1g') \
                         .getOrCreate()
    return spark