# 看看大数据里都有啥
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, to_date, col

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取 Hive 表
hive_df = spark.sql("SELECT * FROM default.users")

# 显示前 5 行数据
hive_df.show(5, truncate=False)
