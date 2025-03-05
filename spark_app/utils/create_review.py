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
create external table if not exists json_review
(
json_body string
) stored as textfile location '/yelp/review'
"""
spark.sql(create_json_user_sql)
spark.sql("USE default")
# 创建 users 表
create_review_sql = """
CREATE TABLE if not exists review
(
review_id string,
rev_user_id string,
rev_business_id string,
rev_stars int,
rev_useful int,
rev_funny int,
rev_cool int,
rev_text string,
rev_timestamp string,
rev_date date
)
"""
spark.sql(create_review_sql)

# 从 json_user 表插入数据到 users 表
insert_users_sql = """
FROM json_review
INSERT
OVERWRITE
TABLE
review
SELECT get_json_object(json_body, '$.review_id'),
get_json_object(json_body,'$.user_id'),
get_json_object(json_body,'$.business_id'),
get_json_object(json_body,'$.stars'),
get_json_object(json_body,'$.useful'),
get_json_object(json_body,'$.funny'),
get_json_object(json_body,'$.cool'),
regexp_replace(regexp_replace(get_json_object(json_body,'$.text'),'\n',' '),'\r',' '),
get_json_object(json_body,'$.date'),
cast(substr(get_json_object(json_body,'$.date'),0,10) as date)
"""
spark.sql(insert_users_sql)

# 验证表是否创建成功
# spark.sql("SHOW TABLES LIKE 'users'").show()

# 查询 users 表数据
spark.sql("SELECT * FROM review LIMIT 10").show()

# 关闭 SparkSession
spark.stop()