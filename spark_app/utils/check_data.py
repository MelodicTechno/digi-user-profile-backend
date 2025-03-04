# 看看大数据里都有啥
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark import find_spark_home

# print(find_spark_home._find_spark_home())
#
# employee_df = spark.read.format("jdbc") \
#     .option("url", "jdbc:mysql://192.168.100.235:3306/tags_dat") \
#     .option("dbtable", "tbl_users") \
#     .option("user", "root") \
#     .option("password", "200456") \
#     .load()
#
# employee_df.show(5,truncate=False)

from pyspark.sql import SparkSession

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取 Hive 表
#hive_df = spark.sql("SELECT * FROM default.business")

#美国最常见商户（前20）
hive_most_common_shop = spark.sql("SELECT default.business.name, COUNT(*) AS shop_count FROM default.business GROUP BY name ORDER BY shop_count DESC")
hive_most_common_shop.show(truncate=False)  # 显示统计结果
# 显示前 5 行数据
#hive_df.show(5, truncate=False)