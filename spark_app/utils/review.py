# 看看大数据里都有啥
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, to_date, col

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取 Hive 表
hive_df = spark.sql("SELECT * FROM default.review")

# 显示前 5 行数据
hive_df.show(5, truncate=False)
#数据清洗 新增一列年份数据

#将字符串格式的日期转为date格式
updated_df = hive_df.withColumn("date", to_date(col("date"),"yyyy-MM-dd")) \
    .withColumn("year",year(col("date")))

updated_df.createOrReplaceTempView("new_review")
# 将更新后的数据写入 Hive 表
# 如果表不存在，可以先创建表
updated_df.write.mode("overwrite").saveAsTable("default.tmp_review")
# 从临时表读取数据并覆盖写入原表
temp_df = spark.sql("SELECT * FROM default.temp_review")
temp_df.write.mode("overwrite").saveAsTable("default.review")
# 查看表结构以验证
spark.sql("DESCRIBE default.review").show(truncate=False)


'''
# 统计每年的评论数
year_review_counts = spark.sql("select year, count(*) as review_counts from review group by year")
year_review_counts.show(truncate=False)
'''



'''
#统计有用（helpful）、有趣（funny）及酷（cool）的评论及数量
#有用
useful_comments = spark.sql("select count(*) from default.review where useful > 0")
#有趣
funny_comments = spark.sql("select count(*) from default.review where funny > 0")
#酷
cool_comments = spark.sql("select count(*) from default.review where cool > 0")
'''



'''
#每年全部评论用户排行榜
user_review_counts = spark.sql("select review.user_id,count(*) as review_counts from review  group by review.user_id order by review_counts desc")
user_review_counts.show(truncate=False)
'''

#从评论中提取最常见的Top20词语

#从评论中提取正面评论（评分>3）的Top10词语
'''
#提取正面评论
positive_review_df = spark.sql(select * from review where review.stars >= 3.0)

# 转换为小写并去除标点符号
positive_reviews_df = positive_reviews_df.withColumn(
    "cleaned_text",
    regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")  # 去除非字母字符
)

# 分词
positive_reviews_df = positive_reviews_df.withColumn(
    "words",
    split(col("cleaned_text"), "\\s+")  # 按空格分词
)

# 去除停用词
stop_words = StopWordsRemover.loadDefaultStopWords("english")  # 加载英文停用词
stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words)
positive_reviews_df = stop_words_remover.transform(positive_reviews_df)

# 展开词语
words_df = positive_reviews_df.select(explode(col("filtered_words")).alias("word"))

# 统计词频
word_counts_df = words_df.groupBy("word").count()

# 按词频排序并提取 Top 10
top_10_words_df = word_counts_df.orderBy(col("count").desc()).limit(10)

# 显示结果
top_10_words_df.show()

'''



'''
#从评论中提取负面评论（评分<=3）的Top10词语
negative_review = spark.sql(select * from review where review.stars < 3.0)
'''
#提取全部评论、通过词性过滤，并完成词云分析（Word Cloud）
#计算单词的关系图（譬如chinese、steak等单词）