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


# 读取Hive表时增加name字段
business_df = spark.sql("SELECT business_id, name, city FROM business")

# 数据关联（与城市统计共用exploded_checkin）
joined_business = exploded_checkin.join(
    business_df,
    "business_id",
    "inner"
)

# 统计商家维度打卡次数（包含名称和城市）
business_ranking = joined_business.groupBy("business_id", "name", "city") \
    .count() \
    .withColumnRenamed("count", "total_checkins") \
    .orderBy(col("total_checkins").desc()) \
    .select("name", "city", "total_checkins")  # 调整字段顺序

# 展示结果（默认显示前20）
business_ranking.show(n=20, truncate=False)