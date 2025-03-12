from pyspark.sql import SparkSession
from pyspark.sql.functions import (year,
                                   to_date,
                                   col, regexp_replace, split, explode, expr, lit, sequence,
                                   sum, count, when, round, to_timestamp, lower,
                                   hour, size)

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

category_counts = spark.sql("""
WITH exploded_categories AS (
    SELECT explode(categories) AS category
    FROM default.business
)
SELECT category, COUNT(*) AS count
FROM exploded_categories
GROUP BY category
ORDER BY count DESC
LIMIT 10
""")

# 显示结果
category_counts.show()

# 停止 SparkSession
spark.stop()