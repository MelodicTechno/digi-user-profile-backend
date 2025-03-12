from pyspark.sql import SparkSession
from pyspark.sql.functions import (year,
                                   to_date,
                                   col, regexp_replace, split, explode, expr, lit, sequence,
                                   sum, count, when, round, to_timestamp, lower,
                                   hour, size)

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

hive_df = spark.sql("SELECT * FROM default.business")
updated_df = hive_df.withColumn(
    "isChinese",
    when(col("categories").contains("Chinese"), 1).otherwise(0)
).withColumn(
    "isAmerican",
    when(col("categories").contains("American"), 1).otherwise(0)
).withColumn(
    "isMexican",
    when(col("categories").contains("Mexican"), 1).otherwise(0)
)

# 将 updated_df 注册为临时视图
updated_df.createOrReplaceTempView("updated_df")
updated_df.write.mode("overwrite").saveAsTable("default.business_with_types")

# # 统计不同类型（中国菜、美式、墨西哥）的餐厅的评分分布
Chinese_review_stars = spark.sql("SELECT stars FROM updated_df WHERE isChinese = 1")

# 定义分段条件
Chinese_review_stars = Chinese_review_stars.withColumn("rating_group",
                                   when((col("stars") >= 0) & (col("stars") < 1), "0-1")
                                   .when((col("stars") >= 1) & (col("stars") < 2), "1-2")
                                   .when((col("stars") >= 2) & (col("stars") < 3), "2-3")
                                   .when((col("stars") >= 3) & (col("stars") < 4), "3-4")
                                   .when((col("stars") >= 4) & (col("stars") <= 5), "4-5")
                                   .otherwise("Invalid")
                                   )

# 统计每个分段的数量
Chinese_review_stars = Chinese_review_stars.groupBy("rating_group").count().orderBy("rating_group")

# 收集结果
Chinese_review_stars = Chinese_review_stars.collect()

# 将结果转换为字典列表
Chinese_review_stars = [{"rating_group": row["rating_group"], "count": row["count"]} for row in Chinese_review_stars]

print(Chinese_review_stars)

# 处理美国餐厅评分分布
American_review_stars = spark.sql("SELECT stars FROM updated_df WHERE isAmerican = 1")

# 定义分段条件
American_review_stars = American_review_stars.withColumn("rating_group",
                                                         when((col("stars") >= 0) & (col("stars") < 1), "0-1")
                                                         .when((col("stars") >= 1) & (col("stars") < 2), "1-2")
                                                         .when((col("stars") >= 2) & (col("stars") < 3), "2-3")
                                                         .when((col("stars") >= 3) & (col("stars") < 4), "3-4")
                                                         .when((col("stars") >= 4) & (col("stars") <= 5), "4-5")
                                                         .otherwise("Invalid")
                                                         )

# 统计每个分段的数量
American_review_stars = American_review_stars.groupBy("rating_group").count().orderBy("rating_group")

# 收集结果
American_review_stars = American_review_stars.collect()

# 将结果转换为字典列表
American_review_stars = [{"rating_group": row["rating_group"], "count": row["count"]} for row in
                         American_review_stars]

print(American_review_stars)

# 处理墨西哥餐厅评分分布
Mexico_review_stars = spark.sql("SELECT stars FROM updated_df WHERE isMexican = 1")

# 定义分段条件
Mexico_review_stars = Mexico_review_stars.withColumn("rating_group",
                                                     when((col("stars") >= 0) & (col("stars") < 1), "0-1")
                                                     .when((col("stars") >= 1) & (col("stars") < 2), "1-2")
                                                     .when((col("stars") >= 2) & (col("stars") < 3), "2-3")
                                                     .when((col("stars") >= 3) & (col("stars") < 4), "3-4")
                                                     .when((col("stars") >= 4) & (col("stars") <= 5), "4-5")
                                                     .otherwise("Invalid")
                                                     )

# 统计每个分段的数量
Mexico_review_stars = Mexico_review_stars.groupBy("rating_group").count().orderBy("rating_group")

# 收集结果
Mexico_review_stars = Mexico_review_stars.collect()

# 将结果转换为字典列表
Mexico_review_stars = [{"rating_group": row["rating_group"], "count": row["count"]} for row in Mexico_review_stars]

print(Mexico_review_stars)