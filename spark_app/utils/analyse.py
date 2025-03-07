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
    five_stars_most = spark.sql("Select business.business_id , business.name ,count(*) as five_stars_counts "
                                "from default.business,default.review "
                                "where business.business_id = review.business_id and review.stars = 5.0 "
                                "group by business.business_id , business.name")

    # 统计每年的评论数
    review_in_year = spark.sql("""
        SELECT YEAR(date) AS year, COUNT(*) AS review_count
        FROM default.review
        GROUP BY YEAR(date)
        ORDER BY year
    """).collect()


    # 商家打卡数排序
    checkin_df = spark.sql("SELECT * FROM default.checkin")
    business_df = spark.sql("SELECT business_id, city FROM default.business")
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
    user_every_year = spark.sql("""
    SELECT 
        YEAR(to_date(yelping_since, 'yyyy-MM-dd')) AS year,
        COUNT(*) AS user_count 
    FROM 
        default.users 
    GROUP BY 
        YEAR(to_date(yelping_since, 'yyyy-MM-dd'))
    """)

    # 统计评论达人（review_count）
    review_count = spark.sql("SELECT user_id, name, review_count FROM default.users order by user_review_count DESC")

    # 统计人气最高的用户（fans）
    fans_most = spark.sql("select user_id, name, fans from default.users order by fans DESC")

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

    # Step 2: 统计每年有评论的用户数
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

    # Step 3: 合并数据并计算沉默用户数和比例
    result = annual_users.join(review_users, annual_users.user_year == review_users.review_year, "left") \
        .withColumn("reviewed_users", when(col("reviewed_users").isNull(), lit(0)).otherwise(col("reviewed_users"))) \
        .withColumn("silent_users", col("total_users") - col("reviewed_users")) \
        .withColumn("silent_ratio", col("silent_users") / col("total_users"))

    # Step 4: 选择需要的列并重命名
    total_and_silent = result.select(
        col("user_year").alias("year"),
        col("total_users"),
        col("reviewed_users"),
        col("silent_users"),
        col("silent_ratio")
    ).orderBy("year")

    # 统计出每年的新用户数、评论数、精英用户、tip数、打卡数
    user_every_year = spark.sql("select count(*) from default.users group by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) order by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) DESC")
    review_count_year = spark.sql("select count(*) from default.review group by YEAR(STR_TO_DATE(data, '%Y-%m-%d')) order by YEAR(STR_TO_DATE(yelping_since, '%Y-%m-%d')) DESC")


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
        "top5_businesses": top5_businesses
    }