# spark_app/analyse_data.py
from pyspark.sql import SparkSession

def analyse_data():
    # 创建SparkSession对象
    spark = SparkSession.builder.appName("AnalyseData").getOrCreate()

    # 读取数据
    data = spark.read.format("csv").option("header", "true").load("data.csv")

    # 执行分析
    result = data.groupBy("ImageId").count()  # 替换为实际的列名

    # 打印结果
    result.show()

    # 关闭SparkSession对象
    spark.stop()
