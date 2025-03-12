"""
用户分析里超级难的那个
"""
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (year,
                                   to_date,
                                   col, regexp_replace, split, explode, expr, lit, sequence,
                                   sum, count, when, round, to_timestamp, lower,
                                   hour, size)

def update_deep():
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()