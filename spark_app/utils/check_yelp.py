from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.100.235:9000") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 测试 HDFS 访问
# hdfs_path = "/yelp/user"
# try:
#     files = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
#         spark.sparkContext._jsc.hadoopConfiguration()
#     ).listStatus(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path))
#     for file in files:
#         print(file.getPath().getName())
# except Exception as e:
#     print(f"Failed to access HDFS: {e}")

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
print("fs.defaultFS:", hadoop_conf.get("fs.defaultFS"))

hdfs_path = "hdfs://192.168.100.235:9000/yelp/user"
try:
    files = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    ).listStatus(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path))
    for file in files:
        print(file.getPath().getName())
except Exception as e:
    print(f"Failed to access HDFS: {e}")