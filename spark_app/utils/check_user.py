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

# 替换 <hive_metastore_host> 为你的 Hive Metastore 主机地址
# 例如：thrift://192.168.100.236:9083

# 读取 Hive 表
hive_df = spark.sql("SELECT * FROM default.json_user")

# 显示前 5 行数据
hive_df.show(5, truncate=False)