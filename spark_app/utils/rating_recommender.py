import pandas as pd
import numpy as np
from pyspark.sql import SparkSession


def load_embeddings(user_file, item_file):
    user_embeddings = pd.read_csv(user_file)
    item_embeddings = pd.read_csv(item_file)
    return user_embeddings, item_embeddings


def recommend_items(user_embeddings, item_embeddings, user_id, top_k=10):
    target_user = user_embeddings[user_embeddings['id'] == user_id]
    if target_user.empty:
        print(f"未找到用户 {user_id} 的嵌入信息。")
        return []
    user_emb = target_user.drop(['id', 'bias'], axis=1).values[0]
    user_bias = target_user['bias'].values[0]

    item_ids = item_embeddings['id'].values
    item_embs = item_embeddings.drop(['id', 'bias'], axis=1).values
    item_biases = item_embeddings['bias'].values

    predictions = []
    for item_emb, item_bias in zip(item_embs, item_biases):
        pred = np.dot(user_emb, item_emb) + user_bias + item_bias
        predictions.append(pred)

    recommendation_df = pd.DataFrame({
        'item_id': item_ids,
        'prediction': predictions
    })

    top_recommendations = recommendation_df.sort_values(by='prediction', ascending=False).head(top_k)
    return top_recommendations['item_id'].values


def get_business_info(business_ids):

    if not business_ids:
        return []

    spark = SparkSession.builder \
        .appName("BusinessInfoQuery") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    # 构造IN查询的字符串
    id_list = ", ".join([f"'{bid}'" for bid in business_ids])
    query = f"""
        SELECT business_id, name, address, city, state, stars, review_count
        FROM business
        WHERE business_id IN ({id_list})
    """

    # 执行查询
    business_df = spark.sql(query)


    dict_results = {
        row["business_id"]: {
            "business_id": row["business_id"],
            "name": row["name"],
            "stars": row["stars"],
            "review_count": row["review_count"]
        }
        for row in business_df.collect()
    }

    return dict_results


if __name__ == "__main__":
    # 嵌入文件路径
    user_file = '/Users/a123/Documents/上程/svd/user_embeddings.csv'
    item_file = '/Users/a123/Documents/上程/svd/item_embeddings.csv'

    # 加载嵌入文件
    user_embeddings, item_embeddings = load_embeddings(user_file, item_file)

    # 目标用户ID
    target_user_id = ''

    # 获取推荐商户ID
    recommended_items = recommend_items(user_embeddings, item_embeddings, target_user_id, top_k=20)
    print(f"推荐商户ID: {recommended_items}")

    # # 创建Spark会话
    # spark = SparkSession.builder \
    #     .appName("BusinessInfoQuery") \
    #     .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    #     .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    #     .enableHiveSupport() \
    #     .getOrCreate()

        # 获取商户详细信息
    business_info = get_business_info(recommended_items.tolist())
    print("\n推荐商户详细信息：")
    for info in business_info:
        print(info)