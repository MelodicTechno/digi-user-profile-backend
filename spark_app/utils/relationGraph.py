import json
import networkx as nx
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter
from pyspark.sql import SparkSession

import nltk

# nltk.download('stopwords')
# nltk.download('punkt')


def relationGraph():
    # 初始化 SparkSession
    spark = SparkSession.builder \
        .appName("HiveExample") \
        .config("spark.sql.warehouse.dir", "user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置停用词和标点符号
    stop_words = set(stopwords.words('english'))
    punctuation = set(['.', ',', '!', '?', ';', ':', '"', "'", '(', ')', '[', ']', '{', '}'])

    # 从 HDFS 读取 JSON 文件并提取文本
    hdfs_path = "hdfs://192.168.100.235:9000/yelp/review/yelp_academic_dataset_review.json"
    reviews_df = spark.read.json(hdfs_path).limit(100)

    # 将文本转换为Python列表
    reviews_list = reviews_df.select("text").rdd.flatMap(lambda row: [row.text]).take(100)

    # 处理每条评论
    word_graph = nx.Graph()
    for review in reviews_list:
        text = review
        words = word_tokenize(text)
        words = [word.lower() for word in words if word.isalpha()]
        words = [word for word in words if word not in stop_words and word not in punctuation]
        word_counts = Counter(words)

        # 添加节点到图中
        for word, count in word_counts.items():
            if word not in word_graph:
                word_graph.add_node(word, count=count)
            else:
                word_graph.nodes[word]['count'] += count

        # 只添加与 'cheese' 相关的边
        if 'cheese' in word_counts:
            for word in word_counts:
                if word == 'cheese':
                    continue
                if word_graph.has_edge('cheese', word):
                    word_graph.edges['cheese', word]['weight'] += 1
                else:
                    word_graph.add_edge('cheese', word, weight=1)

    # 停止 SparkSession
    spark.stop()

    # 准备 ECharts 数据
    echarts_data = {
        "nodes": [{"name": node, "value": data["count"]} for node, data in word_graph.nodes(data=True)],
        "links": [{"source": u, "target": v, "value": d["weight"]} for u, v, d in word_graph.edges(data=True)]
    }

    return echarts_data
