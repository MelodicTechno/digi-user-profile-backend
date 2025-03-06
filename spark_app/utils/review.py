# 看看大数据里都有啥
import networkx as nx
from matplotlib import pyplot as plt
from pyspark.sql import functions as F
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import year, to_date, col, regexp_replace, lower, split, explode,size,expr


# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()
'''
# 读取 Hive 表
hive_df = spark.sql("SELECT * FROM default.review")

# 显示前 5 行数据
#hive_df.show(5, truncate=False)
#数据清洗 新增一列年份数据

#将字符串格式的日期转为date格式
updated_review = hive_df.withColumn("date", to_date(col("date"),"yyyy-MM-dd")) \
    .withColumn("year",year(col("date")))

#updated_df.show(5, truncate=False)
# 将 updated_review 注册为临时视图
updated_review.createOrReplaceTempView("updated_review")

# 统计每年的评论数
year_review_counts = spark.sql("select year, count(*) as review_counts from updated_review group by year")
year_review_counts.show(truncate=False)





#统计有用（helpful）、有趣（funny）及酷（cool）的评论及数量
#有用
useful_comments = spark.sql("select count(*) from updated_review where useful > 0")
useful_comments.show(truncate=False)
#有趣
funny_comments = spark.sql("select count(*) from updated_review where funny > 0")
funny_comments.show(truncate=False)
#酷
cool_comments = spark.sql("select count(*) from updated_review where cool > 0")
cool_comments.show(truncate=False)

'''




'''
#每年全部评论用户排行榜
user_review_counts = spark.sql("select review.user_id , users.name,count(*) as review_counts from users,review where review.user_id = users.user_id group by review.user_id ,users.name order by review_counts desc")
user_review_counts.show(truncate=False)
'''


'''
#从评论中提取最常见的Top20词语
# 从 Hive 表中读取数据
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
top_20_words_df = word_counts_df.orderBy(col("count").desc()).limit(20)

# 显示结果
top_20_words_df.show()
'''


'''
#从评论中提取正面评论（评分>3）的Top10词语

from pyspark.sql.functions import col, regexp_replace, lower, split, explode, size
from pyspark.ml.feature import StopWordsRemover

# 提取正面评论
positive_reviews_df = spark.sql("SELECT * FROM review WHERE stars >= 3.0")

# 转换为小写并去除标点符号
positive_reviews_df = positive_reviews_df.withColumn(
    "cleaned_text",
    regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")  # 去除非字母字符
)

# 分词
positive_reviews_df = positive_reviews_df.withColumn(
    "words",
    split(col("cleaned_text"), "\\s+")  # 按空格分词
)

# 过滤掉 words 列为 null 或空列表的行
positive_reviews_df = positive_reviews_df.filter(col("words").isNotNull() & (size(col("words")) > 0))

# 去除停用词
stop_words = StopWordsRemover.loadDefaultStopWords("english")  # 加载英文停用词
stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words)
positive_reviews_df = stop_words_remover.transform(positive_reviews_df)

# 过滤掉空字符串
positive_reviews_df = positive_reviews_df.withColumn(
    "filtered_words",
    expr("filter(filtered_words, word -> word != '')")  # 过滤掉空字符串
)

# 展开词语
words_df = positive_reviews_df.select(explode(col("filtered_words")).alias("word"))

# 统计词频
word_counts_df = words_df.groupBy("word").count()

# 按词频排序并提取 Top 10
top_10_words_df = word_counts_df.orderBy(col("count").desc()).limit(10)

# 显示结果
top_10_words_df.show()

'''

'''
#从评论中提取负面评论（评分<3）的Top10词语

from pyspark.sql.functions import col, regexp_replace, lower, split, explode, size
from pyspark.ml.feature import StopWordsRemover

# 提取正面评论
negative_reviews_df = spark.sql("SELECT * FROM review WHERE stars <= 3.0")

# 转换为小写并去除标点符号
negative_reviews_df = negative_reviews_df.withColumn(
    "cleaned_text",
    regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")  # 去除非字母字符
)

# 分词
negative_reviews_df = negative_reviews_df.withColumn(
    "words",
    split(col("cleaned_text"), "\\s+")  # 按空格分词
)

# 过滤掉 words 列为 null 或空列表的行
negative_reviews_df = negative_reviews_df.filter(col("words").isNotNull() & (size(col("words")) > 0))

# 去除停用词
stop_words = StopWordsRemover.loadDefaultStopWords("english")  # 加载英文停用词
stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words)
negative_reviews_df = stop_words_remover.transform(negative_reviews_df)

# 过滤掉空字符串
negative_reviews_df = negative_reviews_df.withColumn(
    "filtered_words",
    expr("filter(filtered_words, word -> word != '')")  # 过滤掉空字符串
)

# 展开词语
words_df = negative_reviews_df.select(explode(col("filtered_words")).alias("word"))

# 统计词频
word_counts_df = words_df.groupBy("word").count()

# 按词频排序并提取 Top 10
top_10_words_df = word_counts_df.orderBy(col("count").desc()).limit(10)

# 显示结果
top_10_words_df.show()
'''



'''
#提取全部评论、通过词性过滤，并完成词云分析（Word Cloud）
#保留形容词和名词
import nltk
from nltk import pos_tag
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd

# 初始化 Spark 会话并启用 Hive 支持
# 从 Hive 表中读取数据
from pyspark.sql.functions import col, lower, regexp_replace, explode, split, collect_list
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import networkx as nx
import matplotlib.pyplot as plt

reviews_df = spark.sql("SELECT * FROM review")

# 转换为小写并去除标点符号
reviews_df = reviews_df.withColumn(
    "cleaned_text",
    regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")
)

# 分词
reviews_df = reviews_df.withColumn(
    "words",
    split(col("cleaned_text"), "\\s+")
)

# 将分词后的数据转换为 Pandas DataFrame
words_df = reviews_df.select(explode(col("words"))).toPandas()
words_df.columns = ["word"]

# 下载 NLTK 数据
nltk.download("punkt")
nltk.download("averaged_perceptron_tagger")

# 对词语进行词性标注
words_df["pos_tags"] = words_df["word"].apply(lambda x: pos_tag([x]))

# 过滤出名词（NN）和形容词（JJ）
def filter_pos(tags):
    return any(tag.startswith(("NN", "JJ")) for _, tag in tags)

words_df = words_df[words_df["pos_tags"].apply(filter_pos)]

# 统计词频
word_counts = words_df["word"].value_counts().reset_index()
word_counts.columns = ["word", "count"]

# 将词频数据转换为字典
word_freq = dict(zip(word_counts["word"], word_counts["count"]))

# 生成词云
wordcloud = WordCloud(width=800, height=400, background_color="white").generate_from_frequencies(word_freq)

# 显示词云
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()
'''




#计算单词的关系图（譬如chinese、steak等单词）
# 从 Hive 表中读取数据
reviews_df = spark.sql("SELECT * FROM review")

# 转换为小写并去除标点符号
reviews_df = reviews_df.withColumn(
    "cleaned_text",
    regexp_replace(lower(col("text")), "[^a-zA-Z\\s]", "")
)

# 分词
reviews_df = reviews_df.withColumn(
    "words",
    split(col("cleaned_text"), "\\s+")
)

# 定义滑动窗口
window_spec = Window.partitionBy("review_id").orderBy("word_index")

# 为每个单词添加索引
reviews_df = reviews_df.withColumn(
    "word_index",
    F.row_number().over(window_spec)
)

# 提取单词对
word_pairs_df = reviews_df.withColumn(
    "word_pairs",
    F.expr("""
        transform(
            sequence(1, size(words) - 1),
            i -> array(words[i - 1], words[i])
        )
    """)
).select(explode("word_pairs").alias("word_pair"))

# 过滤掉无效单词对
word_pairs_df = word_pairs_df.filter(
    (col("word_pair")[0] != "") & (col("word_pair")[1] != "")
)

# 统计共现频率
co_occurrence_df = word_pairs_df.groupBy("word_pair").count()

# 过滤掉低频共现对
co_occurrence_df = co_occurrence_df.filter(col("count") > 5)

# 创建图
G = nx.Graph()

# 添加边和权重
for row in co_occurrence_df.collect():
    word1, word2 = row["word_pair"]
    count = row["count"]
    G.add_edge(word1, word2, weight=count)

# 设置图形布局
pos = nx.spring_layout(G, k=0.15, iterations=20)

# 绘制节点
nx.draw_networkx_nodes(G, pos, node_size=50, node_color="lightblue")

# 绘制边
nx.draw_networkx_edges(G, pos, width=1, edge_color="gray")

# 绘制标签
nx.draw_networkx_labels(G, pos, font_size=10, font_family="sans-serif")

# 显示图形
plt.title("Word Co-occurrence Graph")
plt.axis("off")
plt.show()