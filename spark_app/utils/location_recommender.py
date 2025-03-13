import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import DoubleType, IntegerType
from math import radians, sin, cos, sqrt, atan2

def location_recommend(user_latitude, user_longitude):

    # 创建 SparkSession，并启用 Hive 支持
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 定义距离计算函数
    def distance(lon1, lat1, lon2, lat2):
        # 转换为弧度
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return 6371 * c  # 地球半径(km)

    # 定义距离评分函数
    def distance_score(distance):
        if distance <= 10:
            return 5
        elif distance <= 20:
            return 4
        elif distance <= 30:
            return 3
        elif distance <= 40:
            return 2
        else:
            return 1

    # 将 Python 函数注册为 UDF
    distance_udf = udf(distance, DoubleType())
    score_udf = udf(distance_score, IntegerType())

    # 查询商家数据，并计算距离和评分
    business_with_score = spark.sql("SELECT business_id, name, stars, review_count, longitude, latitude FROM default.business") \
        .withColumn("distance", distance_udf(lit(user_longitude), lit(user_latitude), col("longitude"), col("latitude"))) \
        .withColumn("distance_score", score_udf(col("distance")))

    # 注册为视图
    business_with_score.createOrReplaceTempView("business_with_score")

    # 查询五星评论最多的商户，并根据评论数量评分
    five_stars_most = spark.sql("""
        SELECT 
            b.business_id,
            b.name,
            b.stars,
            b.review_count,
            b.distance_score,
            b.distance,
            COUNT(r.review_id) AS five_stars_counts,
            CASE
                WHEN COUNT(r.review_id) < 500 THEN 1
                WHEN COUNT(r.review_id) BETWEEN 500 AND 1500 THEN 2
                WHEN COUNT(r.review_id) BETWEEN 1500 AND 2500 THEN 3
                WHEN COUNT(r.review_id) BETWEEN 2500 AND 3500 THEN 4
                ELSE 5
            END AS rating
        FROM 
            business_with_score b
        JOIN 
            default.review r
        ON 
            b.business_id = r.business_id
        WHERE 
            r.stars = 5.0
        GROUP BY 
            b.business_id, b.name, b.stars, b.review_count, b.distance_score, b.distance
    """)

    five_stars_most.createOrReplaceTempView("five_stars_most")

    # 计算总评分
    all_scores = spark.sql("""
        SELECT 
            business_id,
            name,
            stars,
            distance_score,
            distance,
            rating,
            review_count,
            review_scores,
            (distance_score * 0.5 + stars * 0.2 + review_scores * 0.15 + rating * 0.15) AS total_score
        FROM (
            SELECT 
                b.business_id,
                b.name,
                b.stars,
                b.distance_score,
                b.distance,
                b.rating,
                b.review_count,
                CASE
                    WHEN b.review_count < 500 THEN 1
                    WHEN b.review_count BETWEEN 500 AND 1500 THEN 2
                    WHEN b.review_count BETWEEN 1500 AND 2500 THEN 3
                    WHEN b.review_count BETWEEN 2500 AND 3500 THEN 4
                    WHEN b.review_count BETWEEN 3500 AND 4500 THEN 5
                    WHEN b.review_count BETWEEN 4500 AND 5500 THEN 6
                    ELSE 7
                END AS review_scores
            FROM 
                five_stars_most b
        ) AS subquery
    """)

    # 按总分降序排序
    sorted_scores = all_scores.orderBy(col("total_score").desc()).limit(100)
    # sorted_scores.show()

    # 转换为 JSON 格式并收集结果
    dict_results = {
        row["business_id"]: {
            "business_id": row["business_id"],
            "name": row["name"],
            "stars": row["stars"],
            "distance": row["distance"],
            "total_score": row["total_score"],
            "review_count": row["review_count"]
        }
        for row in sorted_scores.collect()
    }

    # 关闭 SparkSession
    spark.stop()

    return dict_results

if __name__ == "__main__":
    results = location_recommend(36.269593, -87.058943)
    print(results)