# 看看大数据里都有啥
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark import find_spark_home

# print(find_spark_home._find_spark_home())
#
# employee_df = spark.read.format("jdbc") \
#     .option("url", "jdbc:mysql://192.168.100.235:3306/tags_dat") \
#     .option("dbtable", "tbl_users") \
#     .option("user", "root") \
#     .option("password", "200456") \
#     .load()
#
# employee_df.show(5,truncate=False)

from pyspark.sql import SparkSession

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取 Hive 表
hive_df = spark.sql("SELECT * FROM default.business")

# 显示前 5 行数据
hive_df.show(5, truncate=False)

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
#  # 分析每年加入的用户数量
# hive_df = spark.sql("SELECT count(*) as user_count FROM default.users group by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d'))")
# hive_stars_high_city.show(truncate=False)  # 显示统计结果
# # 统计评论达人（review_count）
# hive_df = spark.sql("SELECT * FROM default.users order by user_review_count DESC LIMIT 10")
# hive_stars_high_city.show(truncate=False)  # 显示统计结果
# # 统计人气最高的用户（fans）
# hive_df = spark.sql("select * from default.users order by user_fan DESC LIMIT 1")
# # 统计每年优质用户、普通用户比例
# hive_df = spark.sql("")
# # 显示每年总用户数、沉默用户数(未写评论)的比例
# hive_df = spark.sql("select count(*) from default.users  ")
# # 统计出每年的新用户数、评论数、精英用户、tip数、打卡数
# hive_df = spark.sql("select count(*) from default.users group by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) order by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) DESC")
# hive_df = spark.sql("select count(*) from default.review group by YEAR(STR_TO_DATE(data, '%Y-%m-%d')) order by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) DESC")
