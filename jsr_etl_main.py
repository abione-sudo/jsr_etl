from lib.logger import get_logger
from lib.utils import get_spark_session 
from etl.spark_transfms.practice import users_agg


def main():
    spark = get_spark_session("JSR_ETL_Main")
    logger = get_logger(spark, "JSR_ETL_Main")


    logger.info("starting ETL process")
    try:
        # Example ETL steps
        users_agg(spark) 
        
        logger.info("ETL job completed successfully.")
        spark.stop()
    except Exception as e:
        logger.error(f"ETL job failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()