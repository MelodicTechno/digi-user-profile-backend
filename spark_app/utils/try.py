from pyspark.sql import SparkSession
from pyspark.sql.functions import (year,
                                   to_date,
                                   col, regexp_replace, split, explode, expr, lit, sequence,
                                   sum, count, when, round, to_timestamp, lower,
                                   hour, size, trim)

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

stars_high_city = spark.sql("""
SELECT city, 
       AVG(CAST(stars AS FLOAT)) AS average_stars 
FROM default.business 
GROUP BY city 
ORDER BY average_stars DESC 
LIMIT 10
    """).collect()



print(stars_high_city)
# 停止 SparkSession
spark.stop()