from pyspark.sql import SparkSession
from pyspark import find_spark_home
from pyspark.sql.functions import explode, split, col, to_timestamp, year, hour

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取Hive中的checkin表和business表
checkin_df = spark.sql("SELECT * FROM checkin")
business_df = spark.sql("SELECT business_id, city FROM business")

# 处理checkin表：将逗号分隔的日期字符串拆分为多行
exploded_checkin = checkin_df.withColumn(
    "checkin_time",
    explode(split(col("date"), ",\s*"))  # 使用正则表达式分割逗号+空格
).select("business_id", "checkin_time")

# 关联business表获取城市信息
joined_df = exploded_checkin.join(
    business_df,
    "business_id",
    "inner"
)

# 统计各城市打卡次数并排序
city_ranking = joined_df.groupBy("city") \
    .count() \
    .withColumnRenamed("count", "total_checkins") \
    .orderBy(col("total_checkins").desc())

# 显示结果（实际使用时可存储到Hive或导出）
city_ranking.show(truncate=False)