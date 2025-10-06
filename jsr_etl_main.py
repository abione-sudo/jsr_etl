from lib.logger import get_logger
from lib.utils import get_spark_session 


def main():
    spark = get_spark_session("JSR_ETL_Main")
    logger = get_logger(spark, "JSR_ETL_Main")


    logger.info("starting ETL process")
    try:
        # Example ETL steps
        df = spark.createDataFrame([("Alice", 25)], ["name", "age"])
        logger.info(f"Loaded {df.count()} records.")
        logger.info("ETL job completed successfully.")
    except Exception as e:
        logger.error(f"ETL job failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()