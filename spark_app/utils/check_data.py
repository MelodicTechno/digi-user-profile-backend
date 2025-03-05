from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split, col

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
hive_df = spark.sql("SELECT count(*) FROM default.users")
hive_df.show()
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

#统计category的数量


#统计最多的category及数量（前10）


#收获五星评论最多的商户（前20）（连表查询）

#统计不同类型（中国菜、美式、墨西哥）的餐厅类型及数量
# 替换 <hive_metastore_host> 为你的 Hive Metastore 主机地址
# 例如：thrift://192.168.100.236:9083

#统计不同类型（中国菜、美式、墨西哥）的餐厅的评论数量

#统计不同类型（中国菜、美式、墨西哥）的餐厅的评分分布