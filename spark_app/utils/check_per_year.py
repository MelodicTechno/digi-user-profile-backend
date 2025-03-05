from pyspark.sql import SparkSession
from pyspark import find_spark_home
from pyspark.sql.functions import explode, split, col, to_timestamp, year

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()


# 读取 Hive 表
hive_df = spark.sql("SELECT * FROM default.checkin")

exploded_df = hive_df.select(
    "business_id",
    explode(split(col("date"), ", ")).alias("datetime_str")
)



processed_df = exploded_df.withColumn(
    "datetime",
    to_timestamp(col("datetime_str"), "yyyy-MM-dd HH:mm:ss")
).withColumn("year", year(col("datetime")))


# 按年份统计打卡次数
yearly_counts = processed_df.groupBy("year").count().orderBy("year")
yearly_counts.show()