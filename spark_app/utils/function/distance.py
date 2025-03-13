import math
from django.http import JsonResponse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import DoubleType


def nearby_shop(longitude,latitude):
    # 创建 SparkSession，并启用 Hive 支持
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    def distance(user_longitude, user_latitude, business_longitude, business_latitude):
        return math.sqrt((user_longitude - business_longitude) ** 2 +
                         (user_latitude - business_latitude) ** 2)

    # 将 Python 函数注册为 UDF
    distance_udf = udf(distance, DoubleType())

    # 查询最近的店铺
    nearbyshop = spark.sql("SELECT * FROM default.business") \
        .withColumn("dist", distance_udf(lit(longitude), lit(latitude), col("longitude"), col("latitude"))) \
        .orderBy("dist", ascending=True) \
        .limit(9)

    # 将结果转换为 JSON 格式
    result = nearbyshop.toJSON().collect()
    print(result)
    return JsonResponse(result, safe=False)
nearby_shop(20,30)

