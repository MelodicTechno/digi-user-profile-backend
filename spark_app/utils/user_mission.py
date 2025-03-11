"""
用户分析里超级难的那个
"""
from pyspark.sql.functions import (year,
                                   col, split, explode, count, to_timestamp, countDistinct)

from setup import create_spark


def get_deep():
    spark = create_spark()

    # 读取用户表
    users_df = spark.sql("SELECT * FROM default.users")

    # 读取评论表
    reviews_df = spark.sql("SELECT * FROM default.review")

    # 读取打卡表
    checkins_df = spark.sql("SELECT * FROM default.checkin")

    # 读取 tip 表
    tips_df = spark.sql("SELECT * FROM default.tip")

    # 将 elite 列从字符串转换为数组
    users_df = users_df.withColumn("elite", split(col("elite"), ","))

    # 新用户数
    new_users_df = users_df.withColumn("year", year(col("yelping_since")))
    new_users_count_df = new_users_df.groupBy("year").agg(countDistinct("user_id").alias("new_users"))

    # 评论数
    reviews_df = reviews_df.withColumn("year", year(col("date")))
    review_count_df = reviews_df.groupBy("year").agg(count("review_id").alias("review_count"))

    # 精英用户数
    elite_users_df = users_df.select("user_id", explode("elite").alias("elite_year"))
    elite_users_df = elite_users_df.withColumn("year", col("elite_year"))
    elite_users_count_df = elite_users_df.groupBy("year").agg(countDistinct("user_id").alias("elite_users"))

    # tip 数
    tips_df = tips_df.withColumn("year", year(col("date")))
    tip_count_df = tips_df.groupBy("year").agg(count("user_id").alias("tip_count"))

    # 打卡数
    checkins_df = checkins_df.withColumn("date", explode(split(col("date"), ", ")))
    checkins_df = checkins_df.withColumn("year", year(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")))
    checkin_count_df = checkins_df.groupBy("year").agg(count("date").alias("checkin_count"))

    # 合并所有统计数据
    yearly_statistics_df = new_users_count_df.join(review_count_df, "year", "outer") \
        .join(elite_users_count_df, "year", "outer") \
        .join(tip_count_df, "year", "outer") \
        .join(checkin_count_df, "year", "outer") \
        .orderBy("year")

    # 将 DataFrame 转换为 Pandas DataFrame
    yearly_statistics_pd = yearly_statistics_df.toPandas()

    # 填充缺失值
    yearly_statistics_pd.fillna(0, inplace=True)

    # 将 Pandas DataFrame 转换为字典
    yearly_statistics_dict = yearly_statistics_pd.to_dict(orient='records')

    # 停止 SparkSession
    spark.stop()

    # 打印结果
    # print(yearly_statistics_dict)
    return yearly_statistics_dict
