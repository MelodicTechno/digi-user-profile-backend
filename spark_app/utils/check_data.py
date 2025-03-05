from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split, col, when

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

#读取 Hive 表
hive_df = spark.sql("SELECT * FROM default.business")
hive_df.show(truncate=False)
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
#hive_shop_most_state = spark.sql("SELECT default.business.state, COUNT(*) AS shop_count FROM default.business GROUP BY state ORDER BY shop_count DESC")
#hive_shop_most_state.show(truncate=False)  # 显示统计结果

#美国最常见商户以及平均评分
#hive_most_common_shop = spark.sql("SELECT default.business.name, default.stars,COUNT(*) AS shop_count FROM default.business GROUP BY name ORDER BY shop_count DESC")
#hive_most_common_shop.show(truncate=False)  # 显示统计结果

#统计评分最高的城市（前10）
#hive_stars_high_city = spark.sql("SELECT default.business.city, AVG(default.stars) AS average_stars FROM default.business GROUP BY city ORDER BY average_stars DESC")
#hive_stars_high_city.show(truncate=False)  # 显示统计结果



#收获五星评论最多的商户（前20）（连表查询）
#spark.sql("Select         where ")
#统计不同类型（中国菜、美式、墨西哥）的餐厅类型及数量

# 新增列：是否是 Chinese、American、Mexican
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
#updated_df.write.mode("overwrite").saveAsTable("default.business_with_types")
'''
result_df_C = spark.sql("SELECT isChinese ,count(*) FROM updated_df group by isChinese")
result_df_A = spark.sql("SELECT isAmerican,count(*) FROM updated_df group by isAmerican")
result_df_M = spark.sql("SELECT isMexican,count(*) FROM updated_df group by isMexican")
result_df_C.show(truncate=False)
result_df_A.show(truncate=False)
result_df_M.show(truncate=False)
'''




#统计不同类型（中国菜、美式、墨西哥）的餐厅的评论数量
Chinese_review_count = spark.sql("select name , review_count from updated_df where isChinese = 1")
American_review_count = spark.sql("select name , review_count from updated_df where isAmerican = 1")
Mexico_review_count = spark.sql("select name , review_count from updated_df where isMexican = 1")
Chinese_review_count.show()
American_review_count.show()
Mexico_review_count.show()
#统计不同类型（中国菜、美式、墨西哥）的餐厅的评分分布
Chinese_review_stars = spark.sql("select name , stars from updated_df where isChinese = 1")
American_review_stars = spark.sql("select name , stars from updated_df where isAmerican = 1")
Mexico_review_stars = spark.sql("select name , stars from updated_df where isMexican = 1")
Chinese_review_stars.show()
American_review_stars.show()
Mexico_review_stars.show()