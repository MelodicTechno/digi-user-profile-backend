from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, split, trim, expr, array, when, lit, count
from pyspark.sql import DataFrame


def recommend_common_friends_bfs(target_user_id, max_depth=2):
    """
    基于 BFS 遍历目标用户的好友网络（最多四层），推荐潜在用户并按共同好友比例排序。

    参数:
        target_user_id (str): 目标用户的 user_id。
        max_depth (int): 遍历的层级深度（默认为 4）。

    返回:
        list: 包含推荐用户及其相关信息的字典列表。
    """
    # 初始化 SparkSession
    spark = SparkSession.builder \
        .appName("FriendRecommendationBFS") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .config('spark.driver.memory', '8g').config('spark.executor.memory', '8g').config('spark.driver.maxResultsSize', '0') \
        .enableHiveSupport() \
        .getOrCreate()
    # 加载并预处理用户数据
    users_df = spark.sql("SELECT user_id, friends, name, fans FROM default.users ") \
        .withColumn("friends_array",
                    expr("transform(split(friends, ','), x -> trim(x))")) \
        .withColumn("friends_array",
                    when(col("friends").isNull() | (col("friends") == ""),
                         array().cast("array<string>")).otherwise(col("friends_array")))
    users_df.createOrReplaceTempView("users_df")
    # 步骤 1: 收集目标用户的所有直接好友（第一层）
    target_friends = users_df.filter(col("user_id") == target_user_id) \
        .select(explode("friends_array").alias("friend_id")) \
        .distinct()
    # 初始化 BFS 队列和已访问集合
    visited_users = {target_user_id}  # 排除目标用户自己
    current_layer = target_friends
    all_potential_users = current_layer  # 存储所有潜在推荐用户

    # 步骤 2: 广度优先遍历四层好友网络
    for depth in range(1, max_depth):
        # 获取当前层用户的好友（下一层）
        next_layer = current_layer.join(users_df, col("friend_id") == col("user_id")) \
            .select(explode("friends_array").alias("friend_id")) \
            .filter(~col("friend_id").isin(list(visited_users)))  # 排除已访问用户
        # 更新已访问集合
        visited_users.update(
            [row["friend_id"] for row in next_layer.select("friend_id").distinct().collect()])

        # 合并到潜在用户列表
        all_potential_users = all_potential_users.unionByName(next_layer)

        # 准备下一轮遍历
        current_layer = next_layer

    # 步骤 3: 提取所有潜在推荐用户（排除目标用户和直接好友）
    potential_users = all_potential_users.distinct() \
        .filter(col("friend_id") != target_user_id) \
        .join(target_friends, "friend_id", "left_anti")  # 排除直接好友

    # 步骤 4: 计算潜在用户与
    #
    #
    # 目标用户的共同好友数
    # 获取目标用户的好友列表
    target_friends_list = users_df.filter(col("user_id") == target_user_id) \
        .select("friends_array").first()[0]
    print(target_friends_list)
    # 计算共同好友数
    recommendations = potential_users.alias("pu") \
        .join(users_df.alias("u"), col("pu.friend_id") == col("u.user_id")) \
        .select(
        col("pu.friend_id").alias("recommended_user_id"),  # 要推荐的 user_id
        col("u.name").alias("recommend_user_name"),  # 用户名
        col("u.fans").alias("recommend_fans_number"),
        col("u.friends_array"),
        size(expr(f"array_intersect(u.friends_array, array({','.join(map(repr, target_friends_list))}))")).alias(
            "common_friend_count"),
        size("u.friends_array").alias("friend_count")
    ) \
        .withColumn("common_friend_ratio", col("common_friend_count") / col("friend_count")) \
        .filter(col("common_friend_ratio") > 0)  # 过滤无共同好友的用户

    # 步骤 5: 按共同好友比例排序，取前10名并转换为字典列表
    final_recommendations = recommendations.orderBy(col("common_friend_ratio").desc()).limit(10).collect()
    result = [row.asDict() for row in final_recommendations]

    return result