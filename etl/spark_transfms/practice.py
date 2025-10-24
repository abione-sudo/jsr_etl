from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum


def users_agg(spark):
    df=spark.read.option("header",True).csv("/Users/abeee/data_engineering/data/raw_data/users.csv")
    df_cast=df.withColumn("amount",df["amount"].cast("int"))
    df_agg=df_cast.groupBy("name").agg(sum(col("amount")).alias("total_amount")).orderBy("total_amount",ascending=False)
    df_agg.write.format("delta").mode("overwrite").save("/Users/abeee/data_engineering/data/delta_tables/user_agg")

