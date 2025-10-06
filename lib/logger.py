import os 
from datetime import datetime
import logging

def get_logger(spark=None, app_name='JSR'):
   '''Returns logger object'''
   log_dir=os.path.join(os.getcwd(),"logs")
   os.makedirs(log_dir,exist_ok=True)
   log_file=os.path.join(log_dir, f"{app_name}_{datetime.now():%Y%m%d}.log")

   if spark:
      log4j_logger=spark._jvm.org.apache.log4j
      spark_logger=log4j_logger.LogManager.getLogger(app_name)

      # Also configure Python logger for local file writing
      py_logger=logging.getLogger(app_name)
      if not py_logger.handlers:
            file_handler = logging.FileHandler(log_file)
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
            )
            file_handler.setFormatter(formatter)
            py_logger.addHandler(file_handler)
            py_logger.setLevel(logging.INFO)

            class Duallogger:
                def info(self, msg): 
                    spark_logger.info(msg)
                    py_logger.info(msg)
                def warn(self, msg): 
                    spark_logger.warn(msg)
                    py_logger.warning(msg)
                def error(self, msg, exc_info=False): 
                    spark_logger.error(msg)
                    py_logger.error(msg, exc_info=exc_info)
                def debug(self, msg):
                    spark_logger.debug(msg)
                    py_logger.debug(msg)

            return Duallogger()
   else:
        # No Spark — plain Python logger
        logger = logging.getLogger(app_name)
        if not logger.handlers:
            file_handler = logging.FileHandler(log_file)
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            logger.setLevel(logging.INFO)
        return logger