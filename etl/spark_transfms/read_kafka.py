from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import window,to_timestamp
from delta import configure_spark_with_delta_pip


def read_kafka_topic(spark: SparkSession, topic: str, schema: StructType):
    # Read from kafka topic
    kafka_df = (
                    spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", topic)
                    .option("startingOffsets", "latest")  # use "earliest" for historical reads
                    .load()
                )
    # Convert the binary 'value' column to string
    parsed_df = (
                    kafka_df
                    .selectExpr("CAST(value AS STRING) as json_str")
                    .select(from_json(col("json_str"), user_schema).alias("data"))
                    .select("data.*")
                )
    return parsed_df

if __name__ == "__main__":
    builder= SparkSession.builder.appName("KafkaReadExample") \
     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
     .config("spark.sql.streaming.checkpointLocation", "~/data_engineering/data/checkpoints/user_read_checkpoint") \
     .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
     .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
     .config("spark.sql.shuffle.partitions","4") \
        
    spark=configure_spark_with_delta_pip(builder).getOrCreate()
    
    user_schema = StructType([
                            StructField("user_id", IntegerType()),
                            StructField("user_name", StringType()),
                            StructField("email", StringType())
                            ,StructField("is_active", StringType()),
                            StructField("state", StringType()),
                            StructField("country", StringType()),
                            StructField("created_at", StringType())
                                ])

    df_users = read_kafka_topic(spark, "user_info_topic", user_schema)

    df_users_modified = df_users.withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))


    df_grouped = df_users_modified.groupBy(window(col("created_at"), "5 minutes" , "1 minutes"),col("user_id")).\
                                  count().alias("row_count_per_user_in_wimndow")
    query = (
        df_grouped.writeStream.outputMode("update")
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", "~/data_engineering/data/checkpoints/user_window_agg_checkpoint")
        .option("path", "~/data_engineering/data/delta_tables/user_window_counts")
        .start()
    )

    query.awaitTermination()
    

