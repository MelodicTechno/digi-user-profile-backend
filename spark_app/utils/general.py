from pyspark.sql import functions as F
from pyspark.sql.functions import col, split, size
from pyspark.sql.functions import col, split, size
from pyspark.sql.window import Window

from spark_app.utils.setup import create_spark


def get_general():
    # 假设 create_spark 是一个函数，用于创建 SparkSession
    spark = create_spark()

    # 查询 business 表
    business_id = spark.sql("SELECT business_id, name, city, stars, review_count FROM default.business")

    # 查询 checkin 表并处理日期字段
    business_checkin = spark.sql("SELECT business_id, date FROM checkin") \
        .withColumn("checkin_dates", split(col("date"), ", ")) \
        .withColumn("total_checkins", size("checkin_dates"))

    # 将 business_id 和 business_checkin 进行左连接，并填充 total_checkins 的空值为 0
    combined_df = business_id.join(
        business_checkin, "business_id", "left"
    ).fillna(0, ["total_checkins"])

    # 定义窗口函数
    window_spec = Window.partitionBy("city").orderBy(
        F.desc("stars"),
        F.desc("total_checkins"),
        F.desc("review_count")
    )

    # 选择需要的列，并添加排名列
    result_df = combined_df.select(
        "business_id",
        "name",
        "city",
        "stars",
        "total_checkins",
        "review_count",
        F.row_number().over(window_spec).alias("rank")
    ).filter(F.col("rank") <= 5)

    # 将结果转换为字典
    result_dict = result_df.toPandas().to_dict(orient='records')
    return result_dict
