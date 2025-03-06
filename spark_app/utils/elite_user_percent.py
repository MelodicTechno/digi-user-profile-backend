from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, expr, explode, lit, year, to_date, sequence, array_contains, sum, count, when, round, coalesce


spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()


# 读取 Hive 表
user_df = spark.sql("SELECT * FROM default.users")

# 数据清洗与预处理
processed_df = (
    user_df
    # 处理 elite 为 null 的情况
    .withColumn("elite", regexp_replace(coalesce(col("elite"), lit("")), r"\b20\b", "2020"))
    .withColumn("elite", regexp_replace(col("elite"), r"\s+", ""))
    .withColumn("elite_years", split(col("elite"), ","))
    .withColumn("elite_years", expr("array_distinct(elite_years)"))
    .withColumn("elite_years", expr("filter(elite_years, x -> x != '')"))
    .withColumn("elite_years", expr("TRANSFORM(elite_years, x -> CAST(x AS INT))"))
    .withColumn("elite_years", expr("filter(elite_years, x -> x IS NOT NULL)"))
    .withColumn("reg_year", year(to_date(col("yelping_since"), "yyyy-MM-dd HH:mm:ss")))
)

# 计算最晚年份
max_reg_year = processed_df.agg({"reg_year": "max"}).first()[0]
max_elite_year = processed_df.select(explode("elite_years")).agg({"col": "max"}).first()[0]
last_year = max(max_reg_year, max_elite_year) if max_elite_year else max_reg_year

# 生成用户存在年份序列
users_with_years = processed_df.withColumn(
    "active_years",
    sequence(col("reg_year"), lit(last_year))
)

# 展开年份并标记精英状态
# 展开年份并标记精英状态
yearly_status = (
    users_with_years
    .select(
        "user_id",
        explode("active_years").alias("year"),
        "elite_years"
    )
    .withColumn("is_elite", expr("array_contains(elite_years, year)"))  # 修改此处
)

# 按年聚合统计（修正比例计算逻辑）
result = (
    yearly_status
    .groupBy("year")
    .agg(
        sum(when(col("is_elite"), 1).otherwise(0)).alias("elite_users"),
        count("*").alias("total_users")
    )
    .withColumn("ratio",
        when(col("elite_users") == 0, 0.0)  # 无精英用户时比例为0
        .otherwise(round(col("elite_users") / col("total_users"), 4))
    )
    .orderBy("year")
    .select("year", "ratio")
)

result.show()
spark.stop()