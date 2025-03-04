# 看看大数据里都有啥
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark import find_spark_home

print(find_spark_home._find_spark_home())

employee_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://192.168.100.235:3306/tags_dat") \
    .option("dbtable", "tbl_users") \
    .option("user", "root") \
    .option("password", "200456") \
    .load()

employee_df.show(5,truncate=False)