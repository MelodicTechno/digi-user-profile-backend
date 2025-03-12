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

business_df = spark.read.table("default.business")

# 将 categories 字段从逗号分隔的字符串转换为数组
business_df = business_df.withColumn("categories", split(col("categories"), ","))

# 展开 categories 数组并统计每个类别的出现次数
category_counts = (
    business_df
    .select(explode(col("categories")).alias("category"))
    .withColumn("category", trim(col("category")))  # 去除每个 category 的多余空格
    .groupBy("category")
    .count()  # 统计每个类别的出现次数
    .orderBy(col("count").desc())  # 按出现次数降序排序
    .limit(10)  # 取前10名
)


# 显示结果
category_counts.show()

# 停止 SparkSession
spark.stop()