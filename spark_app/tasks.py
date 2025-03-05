from pyspark.sql import SparkSession
from .utils import analyse

"""
提供项目需要的各种服务
"""

# 初始化需求一的表
def init_all():
    analyse.clean()

# 返回需求一的结果们 包括商户分析 用户分析 评论分析等
def get_statiscs():
    init_all()
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.100.235:9000") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.sql("SELECT * FROM review limit 5")
    res = df.collect()
    spark.stop()
    return res

# 以下为大数据应用开发的函数

# 搜索
def list_nearby_businesses(request, latitude, longitude):
    """
    GET /api/businesses/nearby/?lat=<latitude>&lng=<longitude>
    """
    pass

def get_business_details(request, business_id):
    """
    GET /api/businesses/<business_id>/
    """
    pass

def sort_businesses(request):
    """
    GET /api/businesses/?sort_by=<criteria>
    """
    pass

def filter_businesses(request):
    """
    GET /api/businesses/?distance=<distance>&rating=<rating>&amenities=<amenities>
    """
    pass

# 点评推荐
def get_review_recommendations(request, user_id):
    """
    GET /api/recommendations/reviews/?user_id=<user_id>
    """
    pass

