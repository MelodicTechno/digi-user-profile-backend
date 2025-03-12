from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct, first, col
import json


def get_business(business_id):
    spark = SparkSession.builder \
        .appName("BusinessReviewQuery") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 查询商家信息
    business_df = spark.sql(f"""
        SELECT business_id, name, address, city, state, postal_code, stars, categories, hours
        FROM business 
        WHERE business_id = '{business_id}'
    """)
    if business_df.rdd.isEmpty():
        spark.stop()
        return {}

    # 查询评论信息
    review_df = spark.sql(f"""
        SELECT stars, useful, funny, cool, text, date, business_id
        FROM review 
        WHERE business_id = '{business_id}'
    """)

    # 处理评论数据
    if review_df.rdd.isEmpty():
        review_schema = "business_id string, reviews struct<stars:double, useful:int, funny:int, cool:int, text:string, date:string>"
        review_df = spark.createDataFrame([], schema=review_schema)
    else:
        review_df = review_df.select(
            "business_id",
            struct("stars", "useful", "funny", "cool", "text", "date").alias("reviews")
        )

    # 关联数据并聚合
    joined_df = business_df.join(review_df, "business_id", "left")
    business_columns = ["name", "address", "city", "state", "postal_code", "stars", "categories", "hours"]

    grouped_df = joined_df.groupBy("business_id").agg(
        *[first(col).alias(col) for col in business_columns],
        collect_list("reviews").alias("reviews")
    )

    # 转换为字典
    result = {}
    for row in grouped_df.collect():
        try:
            hours_data = json.loads(row.hours) if row.hours else {}
        except json.JSONDecodeError:
            hours_data = {}

        biz_info = {
            "name": row.name,
            "address": row.address,
            "city": row.city,
            "state": row.state,
            "postal_code": row.postal_code,
            "stars": row.stars,
            "categories": row.categories.split(", ") if row.categories else [],
            "hours": hours_data
        }

        reviews = [{
            "stars": r.stars,
            "useful": r.useful,
            "funny": r.funny,
            "cool": r.cool,
            "text": r.text,
            "date": r.date
        } for r in row.reviews] if row.reviews else []

        result[row.business_id] = {
            "business_info": biz_info,
            "reviews": reviews
        }

    spark.stop()
    return result


# 测试调用
# print(get_business("tUFrWirKiKi_TAnsVWINQQ"))