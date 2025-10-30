from lib.logger import get_logger
from lib.utils import get_spark_session 
from etl.spark_transfms.practice import users_agg


def main():
    spark = get_spark_session("JSR_ETL_Main")
    logger = get_logger(spark, "JSR_ETL_Main")


    logger.info("starting ETL process")
    try:
        # Example ETL steps
        #users_agg(spark) 
        delta_path = "/Users/abeee/data_engineering/data/delta_tables/test_table"
        df_table=spark.read.format("delta").load(delta_path)
        df_table.show()
        #[(1, "Alice", 29),(2, "Bob", 31),(3, "Cathy", 25)]

        df_new_records=spark.createDataFrame([(1, "Alice", 39),(4,"abhi",31)], ["id", "name", "age"])
        

        logger.info("ETL job completed successfully.")
        spark.stop()
    except Exception as e:
        logger.error(f"ETL job failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()