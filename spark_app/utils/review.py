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
updated_df = hive_df.withColumn("date", to_date(col("date")),"yyyy-MM-dd") \
    .withColumn("year",year(col("date")))

updated_df.createOrReplaceTempView("year")
# 将更新后的数据写入 Hive 表
# 如果表不存在，可以先创建表
updated_df.write.mode("overwrite").saveAsTable("default.review")
# 查看表结构以验证
spark.sql("DESCRIBE default.review").show(truncate=False)
# 统计每年的评论数
#spark.sql("select year,")
#统计有用（helpful）、有趣（funny）及酷（cool）的评论及数量
#每年全部评论用户排行榜
#从评论中提取最常见的Top20词语
#从评论中提取正面评论（评分>3）的Top10词语
#从评论中提取负面评论（评分<=3）的Top10词语
#提取全部评论、通过词性过滤，并完成词云分析（Word Cloud）
#计算单词的关系图（譬如chinese、steak等单词）