"""
实现词云图的后端工具函数
"""
from pyspark.sql import SparkSession
import nltk
from nltk.tokenize import word_tokenize

# 查询并处理评论
def process_comments():
    """
    从 Hive 表中查询评论数据，进行分词处理
    """
    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 指定 NLTK 数据的存储路径
    nltk_data_path = 'model/'
    nltk.data.path.append(nltk_data_path)

    # 检查 Punkt Tokenizer 模型是否已经下载
    punkt_path = 'tokenizers/punkt'
    try:
        nltk.data.find(punkt_path)
        print("Punkt Tokenizer 模型已经存在，无需重新下载。")
    except LookupError:
        print("Punkt Tokenizer 模型尚未下载，开始下载...")
        nltk.download('punkt', download_dir=nltk_data_path)

    # 查询 review 表中的 text 字段，提取 50 条数据
    review_df = spark.sql("""
        SELECT text
        FROM default.review
        LIMIT 50
    """)

    # 将 DataFrame 转换为 Pandas DataFrame
    comments_pd = review_df.toPandas()

    # 初始化结果列表
    result = []

    for comment in comments_pd['text']:
        # 分词
        tokens = word_tokenize(comment)

        # 将结果添加到列表
        result.append(tokens)

    # 停止 SparkSession
    spark.stop()

    return result
