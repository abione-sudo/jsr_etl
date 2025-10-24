from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_Name='JSR'):
    builder=SparkSession.builder.appName(app_Name) \
                         .config('spark.driver.memory','1g') \
                         .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
                         .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
                         .config("spark.sql.shuffle.partitions","4")
    
    spark=configure_spark_with_delta_pip(builder).getOrCreate()

    return spark    