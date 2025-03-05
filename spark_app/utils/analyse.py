# analyse.py
from pyspark.sql import SparkSession

spark = None

def init_spark():
    global spark
    if spark is None:
        spark = SparkSession.builder \
            .appName("HiveExample") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.100.235:9000") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
            .enableHiveSupport() \
            .getOrCreate()

def clean():
    init_spark()

    # 美国最常见商户（前20）
    most_common_shop = spark.sql("""
        SELECT name, COUNT(*) AS shop_count
        FROM default.business
        GROUP BY name
        ORDER BY shop_count DESC
        LIMIT 20
    """).collect()

    # 美国商户最多的10个城市
    shop_most_city = spark.sql("""
        SELECT city, COUNT(*) AS shop_count
        FROM default.business
        GROUP BY city
        ORDER BY shop_count DESC
        LIMIT 10
    """).collect()

    # 美国商户最多的前五个州
    shop_most_state = spark.sql("""
        SELECT state, COUNT(*) AS shop_count
        FROM default.business
        GROUP BY state
        ORDER BY shop_count DESC
        LIMIT 5
    """).collect()

    # 美国最常见商户以及平均评分
    common_with_rate = spark.sql("""
        SELECT name, AVG(stars) AS avg_stars
        FROM default.business
        GROUP BY name
        ORDER BY COUNT(*) DESC
        LIMIT 20
    """).collect()

    # 统计评分最高的城市（前10）
    stars_high_city = spark.sql("""
        SELECT city, AVG(stars) AS average_stars
        FROM default.business
        GROUP BY city
        ORDER BY average_stars DESC
        LIMIT 10
    """).collect()

    # 收获五星评论最多的商户（前20）
    most_stars = spark.sql("""
        SELECT b.name, COUNT(r.stars) AS star_count
        FROM default.business b
        JOIN default.review r ON b.id = r.business_id
        WHERE r.stars = 5
        GROUP BY b.name
        ORDER BY star_count DESC
        LIMIT 20
    """).collect()

    # 统计每年的评论数
    review_in_year = spark.sql("""
        SELECT YEAR(date) AS year, COUNT(*) AS review_count
        FROM default.review
        GROUP BY YEAR(date)
        ORDER BY year
    """).collect()

    return {
        "most_common_shop": most_common_shop,
        "shop_most_city": shop_most_city,
        "shop_most_state": shop_most_state,
        "common_with_rate": common_with_rate,
        "stars_high_city": stars_high_city,
        "most_stars": most_stars,
        "review_in_year": review_in_year
    }