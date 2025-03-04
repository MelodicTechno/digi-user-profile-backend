from pyspark.sql import SparkSession
import datetime

spark = SparkSession.builder \
    .appName('learn') \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", 1) \
    .config("spark.sql.warehouse.dir", "hdfs://自己的ip:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://自己的ip:9083") \
    .enableHiveSupport() \
    .getOrCreate()

