# analyse.py
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import explode, split, col, to_timestamp, year, hour
from pyspark.sql.functions import col, regexp_replace, split, expr, explode, lit, year, to_date, sequence, array_contains, sum, count, when, round  # 增加round导入


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
        SELECT YEAR(date) AS year, COUNT(*) AS review_count
        FROM default.review
        GROUP BY YEAR(date)
        ORDER BY year
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
        "tips_per_year": tips_per_year,
        "stars_in_1-5": stars_dist,
        "review_in_week": review_in_week,
        "top5_businesses": top5_businesses,
        # 新加入的
        # 分析每年加入的用户数量
        "new_user_every_year": new_user_every_year,
        # 统计评论达人
        "review_count": review_count,
        # 统计人气最高的用户（fans）
        "fans_most": fans_most,
        # 每年的新用户数
        "user_every_year": user_every_year,
        # 每年的评论数
        "review_count_year": review_count_year,
        # 统计每年的总用户数和沉默用户数
        "total_and_silent": total_and_silent,
    }

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
            SELECT YEAR(date) AS year, COUNT(*) AS review_count
            FROM default.review
            GROUP BY YEAR(date)
            ORDER BY year
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

    # 统计出每年的新用户数、评论数、精英用户、tip数、打卡数
    # user_every_year = spark.sql(
    #     "select count(*) as new_user from default.users group by YEAR(TO_DATE(yelping_since, '%Y-%m-%d')) order by YEAR(TO_DATE(yelping_since, '%Y-%m-%d')) DESC")
    # 每年的评论数
    review_count_year = spark.sql(
        "select count(*) as review from default.review group by YEAR(TO_DATE(date, '%Y-%m-%d')) order by YEAR(TO_DATE(date, '%Y-%m-%d')) DESC")

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
    }