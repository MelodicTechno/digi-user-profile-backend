from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import regexp_replace, lower, col, split, expr, explode, size, to_date, year

from spark_app.utils.setup import create_spark


def poop():
    spark = create_spark()

    """
    统计有用（helpful）、有趣（funny）及酷（cool）的评论及数量
    """

    # 读取 Hive 表
    hive_df = spark.sql("SELECT * FROM default.review")

    # 显示前 5 行数据
    # hive_df.show(5, truncate=False)
    # 数据清洗 新增一列年份数据

    # 将字符串格式的日期转为date格式
    updated_review = hive_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
        .withColumn("year", year(col("date")))

    # updated_df.show(5, truncate=False)
    # 将 updated_review 注册为临时视图
    updated_review.createOrReplaceTempView("updated_review")

    summary = spark.sql("""
        SELECT 'useful' AS type, COUNT(*) AS count FROM updated_review WHERE useful > 0
        UNION ALL
        SELECT 'funny' AS type, COUNT(*) AS count FROM updated_review WHERE funny > 0
        UNION ALL
        SELECT 'cool' AS type, COUNT(*) AS count FROM updated_review WHERE cool > 0
    """).toPandas().to_dict(orient='records')

    """
    从评论中提取正面评论（评分>3）的Top10词语
    """
    positive_reviews_df = spark.sql("SELECT * FROM review WHERE stars > 3.0")

    positive_reviews_df = positive_reviews_df.withColumn(
        "cleaned_text",
        regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")
    )

    positive_reviews_df = positive_reviews_df.withColumn(
        "words",
        split(col("cleaned_text"), "\\s+")
    )

    positive_reviews_df = positive_reviews_df.filter(col("words").isNotNull() & (size(col("words")) > 0))

    stop_words = StopWordsRemover.loadDefaultStopWords("english")
    stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words)
    positive_reviews_df = stop_words_remover.transform(positive_reviews_df)

    positive_reviews_df = positive_reviews_df.withColumn(
        "filtered_words",
        expr("filter(filtered_words, word -> word != '')")
    )

    words_df = positive_reviews_df.select(explode(col("filtered_words")).alias("word"))

    word_counts_df = words_df.groupBy("word").count()

    positive_words_df = word_counts_df.orderBy(col("count").desc()).limit(10).toPandas().to_dict(orient='records')

    """
    从评论中提取负面评论（评分<=3）的Top10词语
    """
    negative_reviews_df = spark.sql("SELECT * FROM review WHERE stars <= 3.0")

    negative_reviews_df = negative_reviews_df.withColumn(
        "cleaned_text",
        regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")
    )

    negative_reviews_df = negative_reviews_df.withColumn(
        "words",
        split(col("cleaned_text"), "\\s+")
    )

    negative_reviews_df = negative_reviews_df.filter(col("words").isNotNull() & (size(col("words")) > 0))

    stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words)
    negative_reviews_df = stop_words_remover.transform(negative_reviews_df)

    negative_reviews_df = negative_reviews_df.withColumn(
        "filtered_words",
        expr("filter(filtered_words, word -> word != '')")
    )

    words_df = negative_reviews_df.select(explode(col("filtered_words")).alias("word"))

    word_counts_df = words_df.groupBy("word").count()

    negative_words_df = word_counts_df.orderBy(col("count").desc()).limit(10).toPandas().to_dict(orient='records')

    spark.stop()

    # 返回结果
    return {
        "summary": summary,
        "positive_words": positive_words_df,
        "negative_words": negative_words_df
    }
