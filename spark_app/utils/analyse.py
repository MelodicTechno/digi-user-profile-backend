# 需求一对应的代码
from pyspark.sql import SparkSession

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.100.235:9000") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 找出美国最常见商户(前20)
create_most_common = """
CREATE TABLE most_common AS
SELECT name, COUNT(*) AS count
FROM business
GROUP BY name
"""

# 执行 SQL 语句
spark.sql(create_most_common)

# 找出美国商户最多的前10个城市
create_most_ten = """
CREATE TABLE most_ten AS
select city ,count(*) from business group by city
"""
# 执行
spark.sql(create_most_ten)

# 找出美国商户最多的前5个州
create_most_state = """
create table most_state as
select state, count(*) from business group by state
"""
spark.sql(create_most_state)

# 找出美国最常见商户，并显示平均评分(前20)
create_common_with_rate = """
create table common_with_rate as
select stars, business， count(*) from business group by name
"""
spark.sql(create_common_with_rate)

# 统计评分最高的城市(前10)
create_highest_rate_city = """
create table highest_rate_city as
select city, avg(stars) from business group by city
"""
spark.sql(create_highest_rate_city)

# 收获五星评论最多的商户(前20)
create_most_stars = """
create table most_stars as
select business.name, count(review.stars)
from business,
join review on business.id = review.business_id
where review.stars = 5
group by review.business_id
order by count(review.stars) desc
"""
spark.sql(create_most_stars)

# 统计每年的评论数
create_review_in_year = """
create table 
"""