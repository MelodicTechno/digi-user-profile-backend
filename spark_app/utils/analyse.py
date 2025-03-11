# analyse.py
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (year,
                                   to_date,
                                   col, regexp_replace, split, explode, expr, lit, sequence,
                                   sum, count, when, round, to_timestamp, lower,
                                   hour, size)

def clean():

    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
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
    five_stars_most = spark.sql("""
            SELECT b.business_id, b.name, COUNT(r.stars) AS five_stars_counts
            FROM default.business AS b
            JOIN default.review AS r ON b.business_id = r.business_id
            WHERE r.stars = 5.0
            GROUP BY b.business_id, b.name
            ORDER BY five_stars_counts DESC
            LIMIT 20
        """).collect()

    # 统计每年的评论数
    review_in_year = spark.sql("""
        SELECT 
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) AS year, 
            COUNT(*) AS review 
        FROM 
            default.review 
        WHERE 
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) IS NOT NULL 
        GROUP BY 
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) 
        ORDER BY 
            YEAR(TO_DATE(date, 'yyyy-MM-dd'))
    """).collect()

    # 商家打卡数排序
    checkin_df = spark.sql("SELECT * FROM default.checkin")
    exploded_checkin = checkin_df.withColumn(
        "checkin_time",
        explode(split(col("date"), ",\s*"))
    ).select("business_id", "checkin_time")

    business_df = spark.sql("SELECT business_id, name, city FROM default.business")
    joined_business = exploded_checkin.join(
        business_df,
        "business_id",
        "inner"
    )

    business_ranking_df = joined_business.groupBy("business_id", "name", "city") \
        .count() \
        .withColumnRenamed("count", "total_checkins") \
        .orderBy(col("total_checkins").desc()) \
        .select("name", "city", "total_checkins")

    # 将结果收集到 Python 列表中，并将每一行转换为字典
    business_ranking = business_ranking_df.collect()
    business_ranking = [row.asDict() for row in business_ranking]  # 将 Row 对象转换为字典



    # 最喜欢打卡的城市
    joined_df = exploded_checkin.join(
        business_df,
        "business_id",
        "inner"
    )
    city_ranking = joined_df.groupBy("city") \
        .count() \
        .withColumnRenamed("count", "total_checkins") \
        .orderBy(col("total_checkins").desc()).collect()
    # 修改
    city_ranking = [row.asDict() for row in city_ranking]

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

    # 按小时分组，统计每小时的打卡次数，并按小时排序
    hourly_counts_df = processed_df.groupBy("hour") \
        .count() \
        .orderBy("hour")

    # 将结果收集到 Python 列表中，并将每一行转换为字典
    hourly_counts = hourly_counts_df.collect()
    hourly_counts = [row.asDict() for row in hourly_counts]  # 将 Row 对象转换为字典



    # 每年打卡数统计
    processed_df = exploded_df.withColumn(
        "datetime",
        to_timestamp(col("datetime_str"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn("year", year(col("datetime")))
    yearly_counts = processed_df.groupBy("year").count().orderBy("year")

    yearly_counts = yearly_counts.collect()
    yearly_counts = [row.asDict() for row in yearly_counts]

    # 精英用户比
    user_df = spark.sql("SELECT * FROM default.users")
    processed_df = (
        user_df
        .withColumn("elite", regexp_replace(col("elite"), r"\b20\b", "2020"))
        .withColumn("elite", regexp_replace(col("elite"), r"\s+", ""))
        .withColumn("elite_years", split(col("elite"), ","))
        .withColumn("elite_years", expr("array_distinct(elite_years)"))
        .withColumn("elite_years", expr("filter(elite_years, x -> x != '')"))
        .withColumn("elite_years", expr("TRANSFORM(elite_years, x -> CAST(x AS INT))"))
        .withColumn("elite_years", expr("filter(elite_years, x -> x IS NOT NULL)"))
        .withColumn("reg_year", year(to_date(col("yelping_since"), "yyyy-MM-dd HH:mm:ss")))
    )

    # 计算最大注册年份和最大精英年份
    max_reg_year = processed_df.agg({"reg_year": "max"}).first()[0]
    max_elite_year = processed_df.select(explode("elite_years")).agg({"col": "max"}).first()[0]
    last_year = max(max_reg_year, max_elite_year) if max_elite_year else max_reg_year

    # 生成用户活跃年份序列
    users_with_years = processed_df.withColumn(
        "active_years",
        sequence(col("reg_year"), lit(last_year))
    )

    # 展开活跃年份并标记是否为精英用户
    yearly_status = (
        users_with_years
        .select(
            "user_id",
            explode("active_years").alias("year"),
            "elite_years"
        )
        .withColumn("is_elite", expr("array_contains(elite_years, year)"))
    )

    # 按年份分组，计算精英用户比例
    elite_user_percent_df = (
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

    # 将结果收集到 Python 列表中，并将每一行转换为字典
    elite_user_percent = elite_user_percent_df.collect()
    elite_user_percent = [row.asDict() for row in elite_user_percent]  # 将 Row 对象转换为字典


    # 每年 tip 数
    tips_per_year = spark.sql("""
        SELECT 
            YEAR(date) AS year,
            COUNT(*) AS tip_count
        FROM 
            default.tip
        WHERE 
            date IS NOT NULL  -- 过滤空值
        GROUP BY 
            YEAR(date)
        ORDER BY 
            year
""").collect()


    # 评分分布（1-5）
    stars_dist = spark.sql("""
    SELECT
        CAST(stars AS INT) AS rating,
        COUNT(*) AS review_count
    FROM default.review
    WHERE stars IS NOT NULL
    GROUP BY CAST(stars AS INT)
    ORDER BY rating
    """)



    # 每周各天的评分次数
    review_in_week = spark.sql("""
    WITH weekday_data AS (
        SELECT 
            date_format(to_date(date), 'EEEE') AS weekday_name,
            CASE 
                WHEN extract(dayofweek FROM to_date(date)) = 1 THEN 7  -- Sunday
                ELSE extract(dayofweek FROM to_date(date)) - 1 
            END AS weekday_num,
            review_id
        FROM default.review
        WHERE date IS NOT NULL
    )
    SELECT 
        weekday_name,
        COUNT(review_id) AS review_count
    FROM weekday_data
    WHERE weekday_num IS NOT NULL
    GROUP BY weekday_name, weekday_num
    ORDER BY weekday_num
    """)


    # 5星评价最多的前5个商家
    top5_businesses = spark.sql("""
    SELECT 
        business_id, 
        COUNT(*) AS five_star_count
    FROM default.review
    WHERE CAST(stars AS INT) = 5 AND stars IS NOT NULL
    GROUP BY business_id
    ORDER BY five_star_count DESC
    LIMIT 5
    """)



    # 分析每年加入的用户数量
    new_user_every_year = spark.sql("""
    SELECT 
        YEAR(to_date(yelping_since, 'yyyy-MM-dd')) AS year,
        COUNT(*) AS user_count 
    FROM 
        default.users 
    GROUP BY 
        YEAR(to_date(yelping_since, 'yyyy-MM-dd'))
    """)
    new_user_every_year = new_user_every_year.collect()
    new_user_every_year = [row.asDict() for row in new_user_every_year]

    # 统计评论达人（review_count）
    review_count = spark.sql("SELECT user_id, name, review_count FROM default.users order by review_count DESC")
    review_count = review_count.collect()
    review_count = [row.asDict() for row in review_count]


    # 统计人气最高的用户（fans）
    fans_most = spark.sql("select user_id, name, fans from default.users order by fans DESC LIMIT 10")
    fans_most = fans_most.collect()
    fans_most = [row.asDict() for row in fans_most]

    # 显示每年总用户数、沉默用户数(未写评论)的比例
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

    # 统计每年的总用户数和沉默用户数
    review_users = spark.sql("""
        SELECT
            YEAR(to_date(date, 'yyyy-MM-dd')) AS review_year,
            COUNT(DISTINCT user_id) AS reviewed_users
        FROM
            default.review
        GROUP BY
            YEAR(to_date(date, 'yyyy-MM-dd'))
        ORDER BY
            review_year
    """)



    result = annual_users.join(review_users, annual_users.user_year == review_users.review_year, "left") \
        .withColumn("reviewed_users", when(col("reviewed_users").isNull(), lit(0)).otherwise(col("reviewed_users"))) \
        .withColumn("silent_users", col("total_users") - col("reviewed_users")) \
        .withColumn("silent_ratio", col("silent_users") / col("total_users"))


    # 统计每年的总用户数和沉默用户数
    total_and_silent = result.select(
        col("user_year").alias("year"),
        col("total_users"),
        col("reviewed_users"),
        col("silent_users"),
        col("silent_ratio")
    ).orderBy("year")

    total_and_silent = total_and_silent.collect()
    total_and_silent = [row.asDict() for row in total_and_silent]

    # 统计出每年的新用户数、评论数
    user_every_year = spark.sql("select count(*) as new_user from default.users group by YEAR(TO_DATE(yelping_since, '%Y-%m-%d')) order by YEAR(TO_DATE(yelping_since, '%Y-%m-%d')) DESC")
    # 每年的评论数
    review_count_year = spark.sql("select count(*) as review from default.review group by YEAR(TO_DATE(date, '%Y-%m-%d')) order by YEAR(TO_DATE(date, '%Y-%m-%d')) DESC")



    spark.stop()


# 对商户统计数据的更新
def update_business():
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
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
    five_stars_most = spark.sql("""
                SELECT b.business_id, b.name, COUNT(r.stars) AS five_stars_counts
                FROM default.business AS b
                JOIN default.review AS r ON b.business_id = r.business_id
                WHERE r.stars = 5.0
                GROUP BY b.business_id, b.name
                ORDER BY five_stars_counts DESC
                LIMIT 20
            """).collect()

    # 统计每年的评论数
    review_in_year = spark.sql("""
        SELECT
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) AS year,
            COUNT(*) AS review_count
        FROM
            default.review
        WHERE
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) IS NOT NULL
        GROUP BY
            YEAR(TO_DATE(date, 'yyyy-MM-dd'))
        ORDER BY
            YEAR(TO_DATE(date, 'yyyy-MM-dd'))
    """).collect()
    review_in_year = [row.asDict() for row in review_in_year]


    # 商家打卡数排序
    checkin_df = spark.sql("SELECT * FROM default.checkin")
    exploded_checkin = checkin_df.withColumn(
        "checkin_time",
        explode(split(col("date"), ",\s*"))
    ).select("business_id", "checkin_time")
    #
    business_df = spark.sql("SELECT business_id, name, city FROM default.business")
    joined_business = exploded_checkin.join(
        business_df,
        "business_id",
        "inner"
    )

    business_ranking_df = joined_business.groupBy("business_id", "name", "city") \
        .count() \
        .withColumnRenamed("count", "total_checkins") \
        .orderBy(col("total_checkins").desc()) \
        .select("name", "city", "total_checkins")

    # 将结果收集到 Python 列表中，并将每一行转换为字典
    business_ranking = business_ranking_df.collect()
    business_ranking = [row.asDict() for row in business_ranking]  # 将 Row 对象转换为字典

    # 最喜欢打卡的城市
    # joined_df = exploded_checkin.join(
    #     business_df,
    #     "business_id",
    #     "inner"
    # )
    # city_ranking = joined_df.groupBy("city") \
    #     .count() \
    #     .withColumnRenamed("count", "total_checkins") \
    #     .orderBy(col("total_checkins").desc()).collect()
    # 修改
    # city_ranking = [row.asDict() for row in city_ranking]

    # ---------------------------------优化
    checkin_df = spark.sql("SELECT * FROM default.checkin")
    business_df = spark.sql("SELECT business_id, name, city FROM default.business")

    exploded_checkin = checkin_df.withColumn(
        "checkin_time",
        explode(split(col("date"), ",\s*"))
    ).select("business_id", "checkin_time")

    joined_df = exploded_checkin.join(business_df, "business_id", "inner")

    city_ranking_df = joined_df.groupBy("city") \
        .count() \
        .withColumnRenamed("count", "total_checkins") \
        .orderBy(col("total_checkins").desc())

    city_ranking = city_ranking_df.collect()
    city_ranking = [row.asDict() for row in city_ranking]

    # ------------------优化


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
    hourly_counts = processed_df.groupBy("hour").count().orderBy("hour").collect()
    hourly_counts = [row.asDict() for row in hourly_counts]



    # 每年打卡数统计
    processed_df = exploded_df.withColumn(
        "datetime",
        to_timestamp(col("datetime_str"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn("year", year(col("datetime")))
    yearly_counts = processed_df.groupBy("year").count().orderBy("year").collect()
    yearly_counts = [row.asDict() for row in yearly_counts]


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
    ).collect()
    elite_user_percent = [row.asDict() for row in elite_user_percent]

    spark.stop()

    # 返回得到的商户统计数据
    return {
        "most_common_shop": most_common_shop,
        "shop_most_city": shop_most_city,
        "shop_most_state": shop_most_state,
        "common_with_rate": common_with_rate,
        "stars_high_city": stars_high_city,
        "most_stars": five_stars_most,
        "review_in_year": review_in_year,
        "business_checkin_ranking": business_ranking,
        "city_checkin_ranking": city_ranking,
        "checkin_per_hour": hourly_counts,
        "checkin_per_year": yearly_counts,
        "elite_user_percent": elite_user_percent,
    }

# 对用户统计数据的更新
def update_users():
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 分析每年加入的用户数量
    new_user_every_year_df = spark.sql("""
        SELECT 
            YEAR(to_date(yelping_since, 'yyyy-MM-dd')) AS year,
            COUNT(*) AS user_count 
        FROM 
            default.users 
        GROUP BY 
            YEAR(to_date(yelping_since, 'yyyy-MM-dd'))
        ORDER BY 
            year
    """)
    new_user_every_year = new_user_every_year_df.collect()
    new_user_every_year = [row.asDict() for row in new_user_every_year]

    # 统计评论达人（review_count）
    review_count_df = spark.sql("""
        SELECT user_id, name, review_count 
        FROM default.users 
        ORDER BY review_count DESC limit 10
    """)
    review_count = review_count_df.collect()
    review_count = [row.asDict() for row in review_count]

    # 统计人气最高的用户（fans）
    fans_most_df = spark.sql("""
        SELECT user_id, name, fans 
        FROM default.users 
        ORDER BY fans DESC limit 10
    """)
    fans_most = fans_most_df.collect()
    fans_most = [row.asDict() for row in fans_most]

    # 显示每年总用户数、沉默用户数(未写评论)的比例
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

    # 统计每年的总用户数和沉默用户数
    review_users = spark.sql("""
            SELECT
                YEAR(to_date(date, 'yyyy-MM-dd')) AS review_year,
                COUNT(DISTINCT user_id) AS reviewed_users
            FROM
                default.review
            GROUP BY
                YEAR(to_date(date, 'yyyy-MM-dd'))
            ORDER BY
                review_year
        """)

    result = annual_users.join(review_users, annual_users.user_year == review_users.review_year, "left") \
        .withColumn("reviewed_users", when(col("reviewed_users").isNull(), lit(0)).otherwise(col("reviewed_users"))) \
        .withColumn("silent_users", col("total_users") - col("reviewed_users")) \
        .withColumn("silent_ratio", col("silent_users") / col("total_users"))

    # 统计每年的总用户数和沉默用户数
    total_and_silent = result.select(
        col("user_year").alias("year"),
        col("total_users"),
        col("reviewed_users"),
        col("silent_users"),
        col("silent_ratio")
    ).orderBy("year")

    total_and_silent = total_and_silent.collect()
    total_and_silent = [row.asDict() for row in total_and_silent]

    # 每年 tip 数
    tips_per_year = spark.sql("""
                SELECT 
                    YEAR(date) AS year,
                    COUNT(*) AS tip_count
                FROM 
                    default.tip
                WHERE 
                    date IS NOT NULL  -- 过滤空值
                GROUP BY 
                    YEAR(date)
                ORDER BY 
                    year
        """).collect()

    # 统计出每年的新用户数、评论数、精英用户、tip数、打卡数
    # user_every_year = spark.sql(
    #     "select count(*) as new_user from default.users group by YEAR(TO_DATE(yelping_since, '%Y-%m-%d')) order by YEAR(TO_DATE(yelping_since, '%Y-%m-%d')) DESC")
    # 每年的评论数
    review_count_year = spark.sql("""
        SELECT 
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) AS year, 
            COUNT(*) AS review 
        FROM 
            default.review 
        WHERE 
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) IS NOT NULL 
        GROUP BY 
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) 
        ORDER BY 
            YEAR(TO_DATE(date, 'yyyy-MM-dd'))
    """)

    review_count_year = review_count_year.collect()
    review_count_year = [row.asDict() for row in review_count_year]
    # 记得停下休息一下~
    spark.stop()

    return {
        # 新加入的
        # 分析每年加入的用户数量
        "new_user_every_year": new_user_every_year,
        # 统计评论达人
        "review_count": review_count,
        # 统计人气最高的用户（fans）
        "fans_most": fans_most,
        # 每年的新用户数
        # "user_every_year": user_every_year,
        # 每年的评论数
        "review_count_year": review_count_year,
        # 统计每年的总用户数和沉默用户数
        "total_and_silent": total_and_silent,
        'tips_per_year': tips_per_year,
    }

# 更新评分
def update_scores():
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 评分分布（1-5）
    stars_dist = spark.sql(
        """ SELECT CAST(stars AS INT) AS rating, COUNT(*) AS review_count FROM default.review 
        WHERE stars IS NOT NULL GROUP BY CAST(stars AS INT) ORDER BY rating """).collect()


    stars_dist = [row.asDict() for row in stars_dist]

    # 每周各天的评分次数
    review_in_week = spark.sql("""
        WITH weekday_data AS (
            SELECT 
                date_format(to_date(date), 'EEEE') AS weekday_name,
                CASE 
                    WHEN extract(dayofweek FROM to_date(date)) = 1 THEN 7  -- Sunday
                    ELSE extract(dayofweek FROM to_date(date)) - 1 
                END AS weekday_num,
                review_id
            FROM default.review
            WHERE date IS NOT NULL
        )
        SELECT 
            weekday_name,
            COUNT(review_id) AS review_count
        FROM weekday_data
        WHERE weekday_num IS NOT NULL
        GROUP BY weekday_name, weekday_num
        ORDER BY weekday_num
        """).collect()

    review_in_week = [row.asDict() for row in review_in_week]

    # 5星评价最多的前5个商家
    top5_businesses = spark.sql("""
    SELECT 
        b.name as name,
        r.business_id as business_id, 
        COUNT(*) AS five_star_count
    FROM default.review r
    JOIN default.business b ON r.business_id = b.business_id
    WHERE CAST(r.stars AS INT) = 5 AND r.stars IS NOT NULL
    GROUP BY r.business_id, b.name
    ORDER BY five_star_count DESC
    LIMIT 5
    """).collect()

    top5_businesses = [row.asDict() for row in top5_businesses]

    spark.stop()

    return {
        'stars_dist': stars_dist,
        'review_in_week': review_in_week,
        'top5_businesses': top5_businesses,
    }


# 评论分析更新
def update_review():
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 第一问
    year_review_counts = spark.sql("""
        SELECT
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) AS year,
            COUNT(*) AS review_count
        FROM
            default.review
        WHERE
            YEAR(TO_DATE(date, 'yyyy-MM-dd')) IS NOT NULL
        GROUP BY
            YEAR(TO_DATE(date, 'yyyy-MM-dd'))
        ORDER BY
            YEAR(TO_DATE(date, 'yyyy-MM-dd'))
    """)

    year_review_counts = year_review_counts.collect()
    year_review_counts = [row.asDict() for row in year_review_counts]


    # 统计有用（helpful）、有趣（funny）及酷（cool）的评论及数量
    # 有用
    useful_comments = spark.sql("select count(*) from updated_review where useful > 0").collect()
    # 有趣
    funny_comments = spark.sql("select count(*) from updated_review where funny > 0").collect()
    # 酷
    cool_comments = spark.sql("select count(*) from updated_review where cool > 0").collect()

    useful_comments = [row.asDict() for row in useful_comments]
    funny_comments = [row.asDict() for row in funny_comments]
    cool_comments = [row.asDict() for row in cool_comments]

    # 每年全部评论用户排行榜
    user_review_counts = spark.sql(
        "select review.user_id , users.name,count(*) as review_counts from users,review "
        "where review.user_id = users.user_id group by review.user_id ,users.name order by review_counts desc").collect()
    user_review_counts = [row.asDict() for row in user_review_counts]

    # 从评论中提取最常见的Top20词语
    # 从 Hive 表中读取数据
    from pyspark.sql.functions import col, regexp_replace, lower, split, explode, size
    from pyspark.ml.feature import StopWordsRemover
    reviews_df = spark.sql("SELECT * FROM review")

    # 转换为小写并去除标点符号
    reviews_df = reviews_df.withColumn(
        "cleaned_text",
        regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")  # 去除非字母字符
    )

    # 分词
    reviews_df = reviews_df.withColumn(
        "words",
        split(col("cleaned_text"), "\\s+")  # 按空格分词
    )

    # 过滤掉 words 列为 null 或空列表的行
    reviews_df = reviews_df.filter(col("words").isNotNull() & (size(col("words")) > 0))

    # 去除停用词
    stop_words = StopWordsRemover.loadDefaultStopWords("english")  # 加载英文停用词
    stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words)
    reviews_df = stop_words_remover.transform(reviews_df)

    # 展开词语
    words_df = reviews_df.select(explode(col("filtered_words")).alias("word"))

    # 统计词频
    word_counts_df = words_df.groupBy("word").count()

    # 按词频排序并提取 Top 20
    top_20_words_df = word_counts_df.orderBy(col("count").desc()).limit(20).collect()
    top_20_words = [row.asDict() for row in top_20_words_df]

    # 从评论中提取正面评论（评分>3）的Top10词语
    #
    # from pyspark.sql.functions import col, regexp_replace, lower, split, explode, size
    # from pyspark.ml.feature import StopWordsRemover
    #
    # # 提取正面评论
    # positive_reviews_df = spark.sql("SELECT * FROM review WHERE stars >= 3.0")
    #
    # # 转换为小写并去除标点符号
    # positive_reviews_df = positive_reviews_df.withColumn(
    #     "cleaned_text",
    #     regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")  # 去除非字母字符
    # )
    #
    # # 分词
    # positive_reviews_df = positive_reviews_df.withColumn(
    #     "words",
    #     split(col("cleaned_text"), "\\s+")  # 按空格分词
    # )
    #
    # # 过滤掉 words 列为 null 或空列表的行
    # positive_reviews_df = positive_reviews_df.filter(col("words").isNotNull() & (size(col("words")) > 0))
    #
    # # 去除停用词
    # stop_words = StopWordsRemover.loadDefaultStopWords("english")  # 加载英文停用词
    # stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words)
    # positive_reviews_df = stop_words_remover.transform(positive_reviews_df)
    #
    # # 过滤掉空字符串
    # positive_reviews_df = positive_reviews_df.withColumn(
    #     "filtered_words",
    #     expr("filter(filtered_words, word -> word != '')")  # 过滤掉空字符串
    # )
    #
    # # 展开词语
    # words_df = positive_reviews_df.select(explode(col("filtered_words")).alias("word"))
    #
    # # 统计词频
    # word_counts_df = words_df.groupBy("word").count()
    #
    # # 按词频排序并提取 Top 10
    # top_10_words_df = word_counts_df.orderBy(col("count").desc()).limit(10).collect()
    # top_10_words = [row.asDict() for row in top_20_words_df]


    # 计算单词的关系图（譬如chinese、steak等单词）
    # 从 Hive 表中读取数据
    # reviews_df = spark.sql("SELECT * FROM review")
    #
    # # 转换为小写并去除标点符号
    # reviews_df = reviews_df.withColumn(
    #     "cleaned_text",
    #     regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")
    # )
    #
    # # 分词
    # reviews_df = reviews_df.withColumn(
    #     "words",
    #     split(col("cleaned_text"), "\\s+")
    # )
    #
    # # 定义滑动窗口
    # window_spec = Window.partitionBy("review_id").orderBy("word_index")
    #
    # from pyspark.sql import functions as F
    # # 为每个单词添加索引
    # reviews_df = reviews_df.withColumn(
    #     "word_index",
    #     F.row_number().over(window_spec)
    # )
    #
    # # 提取单词对
    # word_pairs_df = reviews_df.withColumn(
    #     "word_pairs",
    #     F.expr("""
    #         transform(
    #             sequence(1, size(words) - 1),
    #             i -> array(words[i - 1], words[i])
    #         )
    #     """)
    # ).select(explode("word_pairs").alias("word_pair"))
    #
    # # 过滤掉无效单词对
    # word_pairs_df = word_pairs_df.filter(
    #     (col("word_pair")[0] != "") & (col("word_pair")[1] != "")
    # )
    #
    # # 统计共现频率
    # co_occurrence_df = word_pairs_df.groupBy("word_pair").count()
    #
    # # 过滤掉低频共现对
    # co_occurrence_df = co_occurrence_df.filter(col("count") > 5)
    #
    # nodes = []
    # edges = []
    #
    # # 添加边和权重
    # for row in co_occurrence_df.collect():
    #     word1, word2 = row["word_pair"]
    #     count = row["count"]
    #
    #     # 添加节点
    #     if word1 not in nodes:
    #         nodes.append({"name": word1})
    #     if word2 not in nodes:
    #         nodes.append({"name": word2})
    #
    #     edges.append({"source": word1, "target": word2, "value": count})
    #
    # # 转换为 JSON 格式
    # graph_data = {
    #     "nodes": nodes,
    #     "edges": edges
    # }

    spark.stop()

    return {
        'year_review_counts': year_review_counts,
        'user_review_counts': user_review_counts,
        'top_20_words': top_20_words,
        # 'top_10_words': top_10_words,
        # 'graph_data': graph_data,
    }