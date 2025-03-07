from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DoubleType, MapType, StringType, ArrayType
import math
import json
import ast
import logging


# 初始化SparkSession
def get_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("BusinessCompetitorAnalysis") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://192.168.100.235:9083") \
        .enableHiveSupport() \
        .getOrCreate()


# 地理距离计算UDF
def register_haversine_udf():
    def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371  # 地球平均半径(km)
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        a = (math.sin(dLat / 2)  **  2
        + math.cos(math.radians(lat1))
        *math.cos(math.radians(lat2))
         * math.sin(dLon / 2)  **  2)
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    spark = get_spark_session()
    spark.udf.register("haversine", haversine, DoubleType())


# 属性解析增强逻辑
def enhanced_parse_attributes(attr_str: str) -> dict:
    """支持嵌套结构和键名清洗的属性解析"""

    def clean_keys(obj):
        """递归清洗所有键的非法字符"""
        if isinstance(obj, dict):
            return {k.strip('{} "'): clean_keys(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [clean_keys(e) for e in obj]
        return obj

    try:
        if not attr_str or attr_str.lower() in ["null", "none", ""]:
            return {}

        # 预处理字符串
        normalized = (
            attr_str.strip()
            .replace("'", "\"")
            .replace("u\"", "\"")
            .replace("None", "null")
            .replace("True", "true")
            .replace("False", "false")
            .replace("\\'", "'")
        )

        # 尝试JSON解析
        try:
            parsed = json.loads(normalized)
        except json.JSONDecodeError:
            # 尝试Python字典解析
            try:
                parsed = ast.literal_eval(normalized)
            except:
                parsed = parse_complex_structure(normalized)

        # 深度清洗键名
        return clean_keys(parsed) if parsed else {}
    except Exception as e:
        logging.error(f"属性解析失败: {str(e)}\n原始数据: {attr_str}")
        return {}


def parse_complex_structure(s: str) -> dict:
    """处理非标准格式的嵌套结构"""
    stack = []
    current_dict = {}
    current_key = None
    buffer = []
    in_key = True
    in_string = False
    nest_level = 0

    for char in s + ',':  # 添加结束符
        if char == '"' and not in_string:
            in_string = True
            continue
        elif char == '"' and in_string:
            in_string = False
            continue

        if not in_string:
            if char == '{':
                nest_level += 1
                if nest_level > 1:
                    stack.append((current_dict, current_key))
                    current_dict = {}
                    current_key = None
            elif char == '}':
                nest_level -= 1
                if nest_level == 0:
                    if current_key and buffer:
                        current_dict[current_key.strip('{} "')] = ''.join(buffer).strip()
                        buffer = []
                        current_key = None
                    break
                elif stack:
                    parent_dict, parent_key = stack.pop()
                    parent_dict[parent_key.strip('{} "')] = current_dict
                    current_dict = parent_dict
            elif char == ':':
                if current_key is None:
                    current_key = ''.join(buffer).strip()
                    buffer = []
            elif char == ',':
                if current_key:
                    current_dict[current_key.strip('{} "')] = ''.join(buffer).strip()
                    current_key = None
                    buffer = []
                elif buffer:
                    current_dict[''.join(buffer).strip('{} "')] = True
                    buffer = []
        else:
            buffer.append(char)

    return current_dict


# 注册属性解析UDF
parse_udf = F.udf(enhanced_parse_attributes, MapType(StringType(), StringType()))


# 属性路径展开逻辑
def flatten_attribute_map(d: dict) -> list:
    """递归展开嵌套字典为路径列表"""

    def _flatten(d, parent_key='', sep='.'):
        items = []
        for k, v in d.items():
            clean_k = k.strip('{} "')
            new_key = f"{parent_key}{sep}{clean_k}" if parent_key else clean_k
            if isinstance(v, dict):
                items.extend(_flatten(v, new_key, sep).items())
            else:
                items.append((new_key, str(v)))
        return dict(items)

    try:
        return list(_flatten(d).keys()) if d else []
    except Exception as e:
        logging.error(f"展开属性失败: {str(e)}")
        return []


flatten_udf = F.udf(flatten_attribute_map, ArrayType(StringType()))


# 核心业务逻辑
def find_competitors(target_business_id: str, max_distance_km: int = 30) -> DataFrame:
    spark = get_spark_session()
    register_haversine_udf()

    # 修改后的SQL（注意第19行注释符号）
    return spark.sql(f"""
        WITH target AS (
            SELECT * FROM business WHERE business_id = '{target_business_id}'
        )
        SELECT 
            b.business_id,
            b.name,
            b.stars,
            b.attributes,
            haversine(b.latitude, b.longitude, t.latitude, t.longitude) as distance_km,
            array_intersect(
                split(b.categories, ', '), 
                split(t.categories, ', ')
            ) as common_categories
        FROM business b
        CROSS JOIN target t
        WHERE 
            b.business_id != t.business_id
            AND b.stars >= t.stars  -- 修改此处为SQL注释
            AND size(array_intersect(
                split(b.categories, ', '), 
                split(t.categories, ', ')
            )) > 0
            AND b.latitude BETWEEN t.latitude - 0.3 AND t.latitude + 0.3
            AND b.longitude BETWEEN t.longitude - 0.3 AND t.longitude + 0.3
            AND haversine(b.latitude, b.longitude, t.latitude, t.longitude) <= {max_distance_km}
        ORDER BY distance_km ASC, b.stars DESC
    """)

def calculate_attribute_distribution(competitors_df: DataFrame) -> DataFrame:
    """计算清洗后的属性分布"""
    spark = get_spark_session()

    processed_df = competitors_df \
        .withColumn("parsed_attrs", parse_udf("attributes")) \
        .withColumn("attr_paths", flatten_udf("parsed_attrs")) \
        .selectExpr("explode(attr_paths) as attribute_path")

    total_count = competitors_df.count()

    return processed_df.groupBy("attribute_path") \
        .agg(
        F.count("*").alias("count"),
        (F.count("*") / F.lit(total_count)).alias("proportion")
    ) \
        .orderBy(F.desc("count"))


def calculate_attribute_distribution(competitors_df: DataFrame) -> (DataFrame, str):
    """计算属性分布并返回RestaurantsPriceRange2的最常见值"""
    spark = get_spark_session()

    processed_df = competitors_df \
        .withColumn("parsed_attrs", parse_udf("attributes")) \
        .withColumn("attr_paths", flatten_udf("parsed_attrs")) \
        .select("business_id", F.explode("attr_paths").alias("attribute_path"), "parsed_attrs")

    total_count = competitors_df.count()

    # 处理RestaurantsPriceRange2的值
    price_range_df = processed_df.filter(F.col("attribute_path") == "RestaurantsPriceRange2") \
        .select(F.col("parsed_attrs.RestaurantsPriceRange2").alias("price_range"))

    # 获取最常见的价格区间
    if price_range_df.count() > 0:
        price_counts = price_range_df.groupBy("price_range").count().orderBy(F.desc("count"))
        most_common_price = price_counts.first()[0] if price_counts.count() > 0 else None
    else:
        most_common_price = None

    # 处理其他属性（排除RestaurantsPriceRange2）
    attr_stats = processed_df.filter(F.col("attribute_path") != "RestaurantsPriceRange2") \
        .groupBy("attribute_path") \
        .agg(
        F.count("*").alias("count"),
        (F.count("*") / F.lit(total_count)).alias("proportion")
    ).orderBy(F.desc("count"))

    return attr_stats, most_common_price


def create_comparison_dict(attr_stats: DataFrame, most_common_price: str, target_attrs: dict) -> dict:
    """创建属性比较字典"""
    # 获取高频属性（proportion > 0.1）
    high_freq_attrs = [row.attribute_path for row in attr_stats.filter(F.col("proportion") > 0.1).collect()]

    # 展平目标属性
    flattened_target = flatten_attribute_map(target_attrs)

    # 构建比较字典
    comparison = {attr: attr in flattened_target for attr in high_freq_attrs}

    # 添加价格区间比较
    target_price = target_attrs.get("RestaurantsPriceRange2", "")
    comparison[f"RestaurantsPriceRange:{most_common_price}"] = str(target_price) == str(most_common_price)

    return comparison


# 使用示例
if __name__ == "__main__":
    spark = get_spark_session()
    register_haversine_udf()

    target_business_id = "MTSW4McQd7CbVtyjqoe9mw"

    # 查找竞争对手
    competitors = find_competitors(target_business_id)
    print("找到竞争对手数量:", competitors.count())

    # 计算属性分布和价格区间
    attr_stats, most_common_price = calculate_attribute_distribution(competitors)
    print("\n属性分布统计（排除RestaurantsPriceRange2）:")
    attr_stats.show(100, truncate=False)
    print(f"\n最常见的RestaurantsPriceRange2值: {most_common_price}")

    # 获取目标餐厅属性
    target_df = spark.sql(f"SELECT attributes FROM business WHERE business_id = '{target_business_id}'")
    target_attrs = enhanced_parse_attributes(target_df.first()[0])

    # 创建比较字典
    comparison_dict = create_comparison_dict(attr_stats, most_common_price, target_attrs)
    print("\n属性比较结果:")
    for k, v in comparison_dict.items():
        print(f"{k.ljust(40)} : {v}")

    spark.stop()