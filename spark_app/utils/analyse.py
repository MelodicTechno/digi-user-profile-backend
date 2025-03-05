# analyse.py
from pyspark.sql import SparkSession

def clean():
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.100.235:9000") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

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

    # 分析每年加入的用户数量
    user_every_year = spark.sql("""
    SELECT 
        YEAR(to_date(yelping_since, 'yyyy-MM-dd')) AS year,
        COUNT(*) AS user_count 
    FROM 
        default.users 
    GROUP BY 
        YEAR(to_date(yelping_since, 'yyyy-MM-dd'))
    """)
    # 统计评论达人（review_count）
    review_count = spark.sql("SELECT user_id, name, review_count FROM default.users order by user_review_count DESC")

    # 统计人气最高的用户（fans）
    fans_most = spark.sql("select user_id, name, fans from default.users order by fans DESC")

    # 统计出每年的新用户数、评论数、精英用户、tip数、打卡数
    user_every_year = spark.sql("select count(*) from default.users group by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) order by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) DESC")
    review_count_year = spark.sql("select count(*) from default.review group by YEAR(STR_TO_DATE(data, '%Y-%m-%d')) order by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) DESC")


    spark.stop()

    return {
        "most_common_shop": most_common_shop,
        "shop_most_city": shop_most_city,
        "shop_most_state": shop_most_state,
        "common_with_rate": common_with_rate,
        "stars_high_city": stars_high_city,
        "most_stars": most_stars,
        "review_in_year": review_in_year
    }