from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import size, split, col, year, to_date, sum, when, lit

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()



# hive_df.show(10, truncate=False)
# spark.sql("SHOW TABLES IN default").show()
# # 美国最常见商户（前20）
# hive_most_common_shop = spark.sql("SELECT default.business.name, COUNT(*) AS shop_count FROM default.business GROUP BY name ORDER BY shop_count DESC")
# hive_most_common_shop.show(truncate=False)  # 显示统计结果
#读取 Hive 表
# hive_df = spark.sql("SELECT * FROM default.business")
# hive_df.show(truncate=False)
'''
#数据清洗
#新增category_count列
# 计算每个商户的类别数量
updated_df = hive_df.withColumn("category_count", size(split(col("categories"), ", ")))
# 将更新后的数据保存到临时表
updated_df.write.mode("overwrite").saveAsTable("default.temp_business")
# 从临时表读取数据并覆盖写入原表
temp_df = spark.sql("SELECT * FROM default.temp_business")
temp_df.write.mode("overwrite").saveAsTable("default.business")
'''
#美国最常见商户（前20）
#hive_most_common_shop = spark.sql("SELECT default.business.name, COUNT(*) AS shop_count FROM default.business GROUP BY name ORDER BY shop_count DESC")
#hive_most_common_shop.show(truncate=False)  # 显示统计结果

#美国商户最多的10个城市
#hive_shop_most_city = spark.sql("SELECT default.business.city, COUNT(*) AS shop_count FROM default.business GROUP BY city ORDER BY shop_count DESC")
#hive_shop_most_city.show(truncate=False)  # 显示统计结果

#美国商户最多的前五个州

# hive_shop_most_state = spark.sql("SELECT default.business.state, COUNT(*) AS shop_count FROM default.business GROUP BY state ORDER BY shop_count DESC")
# hive_shop_most_state.show(truncate=False)  # 显示统计结果
#
# #美国最常见商户以及平均评分
# hive_most_common_shop = spark.sql("SELECT default.business.name, default.stars,COUNT(*) AS shop_count FROM default.business GROUP BY name ORDER BY shop_count DESC")
# hive_most_common_shop.show(truncate=False)  # 显示统计结果
#
# #统计评分最高的城市（前10）
# hive_stars_high_city = spark.sql("SELECT default.business.city, AVG(default.stars) AS average_stars FROM default.business GROUP BY city ORDER BY average_stars DESC")
# hive_stars_high_city.show(truncate=False)  # 显示统计结果
#
# #统计category的数量
#
#
# #统计最多的category及数量（前10）
#
#
# #收获五星评论最多的商户（前20）（连表查询）
#
# #统计不同类型（中国菜、美式、墨西哥）的餐厅类型及数量
#
# #统计不同类型（中国菜、美式、墨西哥）的餐厅的评论数量
#
# #统计不同类型（中国菜、美式、墨西哥）的餐厅的评分分布
#
 # 分析每年加入的用户数量
# hive_df = spark.sql("""
#     SELECT
#         YEAR(to_date(yelping_since, 'yyyy-MM-dd')) AS year,
#         COUNT(*) AS user_count
#     FROM
#         default.users
#     GROUP BY
#         YEAR(to_date(yelping_since, 'yyyy-MM-dd'))
# """)
# hive_df.show(truncate=False)  # 显示统计结果
# # 统计评论达人（review_count）
# hive_df = spark.sql("SELECT user_id, name, review_count FROM default.users order by review_count DESC")
# hive_df.show(truncate=False)  # 显示统计结果
# # 统计人气最高的用户（fans）
# hive_df = spark.sql("select user_id, name, fans from default.users order by fans DESC")
# hive_df.show(truncate=False)  # 显示统计结果
# # 统计每年优质用户、普通用户比例
# hive_df = spark.sql("")
# # 显示每年总用户数、沉默用户数(未写评论)的比例
# hive_df_temp = spark.sql("select * from default.users")
# users = hive_df_temp.withColumn("year", year(to_date(col("yelping_since"), "yyyy-MM-dd")))
# users.write.mode("overwrite").saveAsTable("default.temp_users")

annual_users = spark.sql("""
    SELECT
        YEAR(to_date(yelping_since, 'yyyy-MM-dd')) AS year,
        COUNT(DISTINCT user_id) AS new_users
    FROM
        default.users
    GROUP BY
        YEAR(to_date(yelping_since, 'yyyy-MM-dd'))
    ORDER BY
        year
""").withColumnRenamed("year", "user_year")

window_spec = Window.orderBy("user_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
annual_users = annual_users.withColumn("total_users", sum("new_users").over(window_spec))

# Step 2: 统计每年有评论的用户数
review_users = spark.sql("""
    SELECT
        YEAR(to_date(date, 'yyyy-MM-dd')) AS review_year,
        COUNT(DISTINCT user_id) AS reviewed_users
    FROM
        review
    GROUP BY
        YEAR(to_date(date, 'yyyy-MM-dd'))
    ORDER BY
        review_year
""")

# Step 3: 合并数据并计算沉默用户数和比例
result = annual_users.join(review_users, annual_users.user_year == review_users.review_year, "left") \
                     .withColumn("reviewed_users", when(col("reviewed_users").isNull(), lit(0)).otherwise(col("reviewed_users"))) \
                     .withColumn("silent_users", col("total_users") - col("reviewed_users")) \
                     .withColumn("silent_ratio", col("silent_users") / col("total_users"))

# Step 4: 选择需要的列并重命名
result = result.select(
    col("user_year").alias("year"),
    col("total_users"),
    col("reviewed_users"),
    col("silent_users"),
    col("silent_ratio")
).orderBy("year")

# 显示结果
result.show(truncate=False)



# # 统计出每年的新用户数、评论数、精英用户、tip数、打卡数
# hive_df = spark.sql("""
#     SELECT
#         YEAR(to_date(yelping_since, 'yyyy-MM-dd')) AS year,
#         COUNT(DISTINCT user_id) AS total_users
#     FROM
#         default.users
#     GROUP BY
#         YEAR(to_date(yelping_since, 'yyyy-MM-dd'))
#     ORDER BY
#         year
# """)

# hive_df = spark.sql("select count(*) from default.review group by YEAR(STR_TO_DATE(data, '%Y-%m-%d')) order by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) DESC")

#hive_shop_most_state = spark.sql("SELECT default.business.state, COUNT(*) AS shop_count FROM default.business GROUP BY state ORDER BY shop_count DESC")
#hive_shop_most_state.show(truncate=False)  # 显示统计结果

#美国最常见商户以及平均评分
#hive_most_common_shop = spark.sql("SELECT default.business.name, default.stars,COUNT(*) AS shop_count FROM default.business GROUP BY name ORDER BY shop_count DESC")
#hive_most_common_shop.show(truncate=False)  # 显示统计结果

#统计评分最高的城市（前10）
#hive_stars_high_city = spark.sql("SELECT default.business.city, AVG(default.stars) AS average_stars FROM default.business GROUP BY city ORDER BY average_stars DESC")
#hive_stars_high_city.show(truncate=False)  # 显示统计结果

#统计category的数量


#统计最多的category及数量（前10）


#收获五星评论最多的商户（前20）（连表查询）

#统计不同类型（中国菜、美式、墨西哥）的餐厅类型及数量
# 替换 <hive_metastore_host> 为你的 Hive Metastore 主机地址
# 例如：thrift://192.168.100.236:9083

#统计不同类型（中国菜、美式、墨西哥）的餐厅的评论数量

#统计不同类型（中国菜、美式、墨西哥）的餐厅的评分分布
