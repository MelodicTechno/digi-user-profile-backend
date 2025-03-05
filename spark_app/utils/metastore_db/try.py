from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取JSON
json_path = "file:///Users/a123/Documents/上程/yelp/yelp_academic_dataset_user.json"
df = spark.read.json(json_path)

df.write.mode("overwrite").saveAsTable("default.users")

spark.stop()