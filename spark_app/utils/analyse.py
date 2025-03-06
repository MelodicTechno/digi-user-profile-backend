# analyse.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, to_timestamp, year, hour
from pyspark.sql.functions import col, regexp_replace, split, expr, explode, lit, year, to_date, sequence, array_contains, sum, count, when, round  # 增加round导入


def clean():
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.100.235:9000") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 美国最常见商户（前20）
    most_common_shop = spark.sql("""
        SELECT name, COUNT(*) AS shop_count
        FROM default.business
        GROUP BY name
        ORDER BY shop_count DESC
        LIMIT 20
    """).collect()

    # 美国商户最多的10个城市
    shop_most_city = spark.sql("""
        SELECT city, COUNT(*) AS shop_count
        FROM default.business
        GROUP BY city
        ORDER BY shop_count DESC
        LIMIT 10
    """).collect()

    # 美国商户最多的前五个州
    shop_most_state = spark.sql("""
        SELECT state, COUNT(*) AS shop_count
        FROM default.business
        GROUP BY state
        ORDER BY shop_count DESC
        LIMIT 5
    """).collect()

    # 美国最常见商户以及平均评分
    common_with_rate = spark.sql("""
        SELECT name, AVG(stars) AS avg_stars
        FROM default.business
        GROUP BY name
        ORDER BY COUNT(*) DESC
        LIMIT 20
    """).collect()

    # 统计评分最高的城市（前10）
    stars_high_city = spark.sql("""
        SELECT city, AVG(stars) AS average_stars
        FROM default.business
        GROUP BY city
        ORDER BY average_stars DESC
        LIMIT 10
    """).collect()

    # 收获五星评论最多的商户（前20）
    most_stars = spark.sql("""
        SELECT b.name, COUNT(r.stars) AS star_count
        FROM default.business b
        JOIN default.review r ON b.id = r.business_id
        WHERE r.stars = 5
        GROUP BY b.name
        ORDER BY star_count DESC
        LIMIT 20
    """).collect()

    # 统计每年的评论数
    review_in_year = spark.sql("""
        SELECT YEAR(date) AS year, COUNT(*) AS review_count
        FROM default.review
        GROUP BY YEAR(date)
        ORDER BY year
    """).collect()


    # 商家打卡数排序
    checkin_df = spark.sql("SELECT * FROM checkin")
    business_df = spark.sql("SELECT business_id, city FROM business")
    exploded_checkin = checkin_df.withColumn(
        "checkin_time",
        explode(split(col("date"), ",\s*"))
    ).select("business_id", "checkin_time")
    business_df = spark.sql("SELECT business_id, name, city FROM business")
    joined_business = exploded_checkin.join(
        business_df,
        "business_id",
        "inner"
    )
    business_ranking = joined_business.groupBy("business_id", "name", "city") \
        .count() \
        .withColumnRenamed("count", "total_checkins") \
        .orderBy(col("total_checkins").desc()) \
        .select("name", "city", "total_checkins")



    # 最喜欢打卡的城市
    joined_df = exploded_checkin.join(
        business_df,
        "business_id",
        "inner"
    )
    city_ranking = joined_df.groupBy("city") \
        .count() \
        .withColumnRenamed("count", "total_checkins") \
        .orderBy(col("total_checkins").desc())



    # 每小时打卡数统计
    hive_df = spark.sql("SELECT * FROM default.checkin")
    exploded_df = hive_df.select(
        "business_id",
        explode(split(col("date"), ", ")).alias("datetime_str")
    )
    processed_df = exploded_df.withColumn(
        "datetime",
        to_timestamp(col("datetime_str"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn("year", year(col("datetime")))
    processed_df = processed_df.withColumn("hour", hour(col("datetime")))
    hourly_counts = processed_df.groupBy("hour").count().orderBy("hour")



    # 每年打卡数统计
    processed_df = exploded_df.withColumn(
        "datetime",
        to_timestamp(col("datetime_str"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn("year", year(col("datetime")))
    yearly_counts = processed_df.groupBy("year").count().orderBy("year")




    # 精英用户比
    user_df = spark.sql("SELECT * FROM default.users")
    processed_df = (
        user_df
        .withColumn("elite", regexp_replace(col("elite"), r"\b20\b", "2020"))
        .withColumn("elite", regexp_replace(col("elite"), r"\s+", ""))
        .withColumn("elite_years", split(col("elite"), ","))
        .withColumn("elite_years", expr("array_distinct(elite_years)"))
        .withColumn("elite_years", expr("filter(elite_years, x -> x != '')"))
        # 修改这里的关键转换逻辑
        .withColumn("elite_years", expr("TRANSFORM(elite_years, x -> CAST(x AS INT))"))
        .withColumn("elite_years", expr("filter(elite_years, x -> x IS NOT NULL)"))
        .withColumn("reg_year", year(to_date(col("yelping_since"), "yyyy-MM-dd HH:mm:ss")))
    )
    max_reg_year = processed_df.agg({"reg_year": "max"}).first()[0]
    max_elite_year = processed_df.select(explode("elite_years")).agg({"col": "max"}).first()[0]
    last_year = max(max_reg_year, max_elite_year) if max_elite_year else max_reg_year
    users_with_years = processed_df.withColumn(
        "active_years",
        sequence(col("reg_year"), lit(last_year))
    )
    yearly_status = (
        users_with_years
        .select(
            "user_id",
            explode("active_years").alias("year"),
            "elite_years"
        )
        .withColumn("is_elite", expr("array_contains(elite_years, year)"))
    )
    elite_user_percent = (
        yearly_status
        .groupBy("year")
        .agg(
            sum(when(col("is_elite"), 1).otherwise(0)).alias("elite_users"),
            count("*").alias("total_users")
        )
        .withColumn("ratio",
                    when(col("total_users") == col("elite_users"), 0.0)
                    .otherwise(round(col("elite_users") / (col("total_users") - col("elite_users")), 4))
                    )
        .orderBy("year")
        .select("year", "ratio")
    )

    spark.stop()

    return {
        "most_common_shop": most_common_shop,
        "shop_most_city": shop_most_city,
        "shop_most_state": shop_most_state,
        "common_with_rate": common_with_rate,
        "stars_high_city": stars_high_city,
        "most_stars": most_stars,
        "review_in_year": review_in_year,
        "business_checkin_ranking": business_ranking,
        "city_checkin_ranking": city_ranking,
        "checkin_per_hour": hourly_counts,
        "checkin_per_year": yearly_counts,
        "elite_user_percent": elite_user_percent
    }