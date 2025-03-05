from pyspark.sql import SparkSession

# 创建 SparkSession，并启用 Hive 支持
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.100.235:9000") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 创建外部表 json_user
create_json_user_sql = """
CREATE EXTERNAL TABLE IF NOT EXISTS json_user (
    json_body STRING
)
STORED AS TEXTFILE
LOCATION '/yelp/user'
"""
spark.sql(create_json_user_sql)
spark.sql("USE default")
# 创建 users 表
create_users_sql = """
CREATE TABLE IF NOT EXISTS default.users (
    user_id STRING,
    user_name STRING,
    user_review_count INT,
    user_yelping_since STRING,
    user_friends STRING,
    user_useful INT,
    user_funny INT,
    user_cool INT,
    user_fans INT,
    user_elite STRING,
    user_average_stars FLOAT,
    user_compliment_hot INT,
    user_compliment_more INT,
    user_compliment_profile INT,
    user_compliment_cute INT,
    user_compliment_list INT,
    user_compliment_note INT,
    user_compliment_plain INT,
    user_compliment_cool INT,
    user_compliment_funny INT,
    user_compliment_writer INT,
    user_compliment_photos INT
)
STORED AS PARQUET
"""
spark.sql(create_users_sql)

# 从 json_user 表插入数据到 users 表
insert_users_sql = """
INSERT OVERWRITE TABLE default.users
SELECT 
    CAST(get_json_object(json_body, '$.user_id') AS STRING) AS user_id,
    CAST(get_json_object(json_body, '$.name') AS STRING) AS user_name,
    CAST(get_json_object(json_body, '$.review_count') AS INT) AS user_review_count,
    CAST(get_json_object(json_body, '$.yelping_since') AS STRING) AS user_yelping_since,
    CAST(get_json_object(json_body, '$.friends') AS STRING) AS user_friends,
    CAST(get_json_object(json_body, '$.useful') AS INT) AS user_useful,
    CAST(get_json_object(json_body, '$.funny') AS INT) AS user_funny,
    CAST(get_json_object(json_body, '$.cool') AS INT) AS user_cool,
    CAST(get_json_object(json_body, '$.fans') AS INT) AS user_fans,
    CAST(get_json_object(json_body, '$.elite') AS STRING) AS user_elite,
    CAST(get_json_object(json_body, '$.average_stars') AS FLOAT) AS user_average_stars,
    CAST(get_json_object(json_body, '$.compliment_hot') AS INT) AS user_compliment_hot,
    CAST(get_json_object(json_body, '$.compliment_more') AS INT) AS user_compliment_more,
    CAST(get_json_object(json_body, '$.compliment_profile') AS INT) AS user_compliment_profile,
    CAST(get_json_object(json_body, '$.compliment_cute') AS INT) AS user_compliment_cute,
    CAST(get_json_object(json_body, '$.compliment_list') AS INT) AS user_compliment_list,
    CAST(get_json_object(json_body, '$.compliment_note') AS INT) AS user_compliment_note,
    CAST(get_json_object(json_body, '$.compliment_plain') AS INT) AS user_compliment_plain,
    CAST(get_json_object(json_body, '$.compliment_cool') AS INT) AS user_compliment_cool,
    CAST(get_json_object(json_body, '$.compliment_funny') AS INT) AS user_compliment_funny,
    CAST(get_json_object(json_body, '$.compliment_writer') AS INT) AS user_compliment_writer,
    CAST(get_json_object(json_body, '$.compliment_photos') AS INT) AS user_compliment_photos
FROM default.json_user
"""
spark.sql(insert_users_sql)

# 验证表是否创建成功
spark.sql("SHOW TABLES LIKE 'users'").show()

# 查询 users 表数据
spark.sql("SELECT * FROM default.users LIMIT 10").show()

# 关闭 SparkSession
spark.stop()