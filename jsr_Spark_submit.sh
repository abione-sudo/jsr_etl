spark-submit \
  --master local[1] \
  --files conf/log4j.properties \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" \
  jsr_etl_main.py