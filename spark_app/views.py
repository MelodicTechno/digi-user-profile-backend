from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from .utils import analyse
from .utils.business_recommend import *
from django.core.cache import cache
from .models import (
    MostCommonShop,
    ShopMostCity,
    ShopMostState,
    CommonWithRate,
    StarsHighCity,
    MostStars,
    ReviewInYear,
    BusinessCheckinRanking,
    CityCheckinRanking,
    CheckinPerHour,
    CheckinPerYear,
    EliteUserPercent,
    NewUserEveryYear,
    ReviewCount,
    FanMost,
    UserEveryYear,
    ReviewCountYear,
    TotalAndSilent, ReviewInWeek, StarsDistribution, Top5Businesses, YearReviewCount, UserReviewCount, TopWord,
    GraphNode, GraphEdge,
)
from .utils.analyse import update_review, update_checkin


# 初始化和统计
@require_http_methods(['GET'])
def for_test(request):
    return JsonResponse({"message": "hello from django"})

@require_http_methods(['GET'])
def update_statistics(request):
    statistics = analyse.clean()

    # 清空现有数据
    ReviewCount.objects.all().delete()
    FanMost.objects.all().delete()
    UserEveryYear.objects.all().delete()
    ReviewCountYear.objects.all().delete()
    TotalAndSilent.objects.all().delete()

    # 统计评论达人
    for review in statistics['review_count']:
        ReviewCount.objects.create(
            user_id=review['user_id'],
            name=review['name'],
            review_count=review['review_count'],
        )

    # 统计人气最高的用户（fans）
    for fans in statistics['fans_most']:
        FanMost.objects.create(
            user_id=fans['user_id'],
            name=fans['name'],
            fans=fans['name']
        )

    # 每年的新用户数
    for user in statistics['user_every_year']:
        UserEveryYear.objects.create(
            new_user=user['new_user']
        )

    # 每年的评论数
    for review_count in statistics['review_count_year']:
        ReviewCountYear.objects.create(
            review=review_count['review']
        )

    # 统计每年的总用户数和沉默用户数
    for tas in statistics['total_and_silent']:
        TotalAndSilent.objects.create(
            year=tas['year'],
            total_users=tas['total_users'],
            reviewed_users=tas['reviewed_users'],
            silent_users=tas['silent_users'],
            silent_ratio=tas['silent_ratio'],
        )

    return JsonResponse({"message": "Update data succeeded"})

# 更新商户统计数据
@require_http_methods(['GET'])
def update_business_statistics(request):

    # 更新数据
    try:
        statistics = analyse.update_business()
    except Exception as e:
        return JsonResponse({"message": f"Failed to update business data: {str(e)}"}, status=500)

    # 清空现有数据
    MostCommonShop.objects.all().delete()
    ShopMostCity.objects.all().delete()
    ShopMostState.objects.all().delete()
    CommonWithRate.objects.all().delete()
    StarsHighCity.objects.all().delete()
    MostStars.objects.all().delete()
    ReviewInYear.objects.all().delete()
    BusinessCheckinRanking.objects.all().delete()
    CityCheckinRanking.objects.all().delete()
    CheckinPerHour.objects.all().delete()
    CheckinPerYear.objects.all().delete()
    EliteUserPercent.objects.all().delete()

    # 保存数据
    for shop in statistics['most_common_shop']:
        MostCommonShop.objects.create(name=shop[0], shop_count=shop[1])

    for city in statistics['shop_most_city']:
        ShopMostCity.objects.create(city=city[0], shop_count=city[1])

    for state in statistics['shop_most_state']:
        ShopMostState.objects.create(state=state[0], shop_count=state[1])

    for rate in statistics['common_with_rate']:
        CommonWithRate.objects.create(name=rate[0], avg_stars=rate[1])

    for city in statistics['stars_high_city']:
        StarsHighCity.objects.create(city=city[0], average_stars=city[1])

    for stars in statistics['most_stars']:
        MostStars.objects.create(business_id=stars[0], business_name=stars[1], five_stars_counts=stars[2])

    for review in statistics['review_in_year']:
        ReviewInYear.objects.create(
            year=review['year'],
            review_count=review['review_count'],
        )

    for ranking in statistics['business_checkin_ranking']:
        BusinessCheckinRanking.objects.create(
            name=ranking['name'],
            city=ranking['city'],
            total_checkins=ranking['total_checkins']
        )

    for ranking in statistics['city_checkin_ranking']:
        CityCheckinRanking.objects.create(
            city=ranking['city'],
            total_checkins=ranking['total_checkins']
        )

    for count in statistics['checkin_per_hour']:
        CheckinPerHour.objects.create(
            hour=count['hour'],
            checkin_count=count['count']
        )

    for count in statistics['checkin_per_year']:
        CheckinPerYear.objects.create(
            year=count['year'],
            checkin_count=count['count']
        )

    for ratio in statistics['elite_user_percent']:
        EliteUserPercent.objects.create(
            year=ratio['year'],
            ratio=ratio['ratio']
        )

    return JsonResponse({"message": "Update business data succeeded"})

# 更新用户统计数据
@require_http_methods(['GET'])
def update_user_statistics(request):
    # 更新数据
    try:
        statistics = analyse.update_users()
    except Exception as e:
        return JsonResponse({"message": f"Failed to update user data: {str(e)}"}, status=500)

    # 清空现有数据
    NewUserEveryYear.objects.all().delete()
    ReviewCount.objects.all().delete()
    FanMost.objects.all().delete()
    UserEveryYear.objects.all().delete()
    ReviewCountYear.objects.all().delete()
    TotalAndSilent.objects.all().delete()

    # 分析每年加入的用户数量
    for new_user in statistics['new_user_every_year']:
        NewUserEveryYear.objects.create(
            year=new_user['year'],
            user_count=new_user['user_count']
        )

    # 统计评论达人
    for review in statistics['review_count']:
        ReviewCount.objects.create(
            user_id=review['user_id'],
            name=review['name'],
            review_count=review['review_count'],
        )

    # 统计人气最高的用户（fans）
    for fans in statistics['fans_most']:
        FanMost.objects.create(
            user_id=fans['user_id'],
            name=fans['name'],
            fans=fans['fans']
        )


    # 每年的新用户数
    for user in statistics['user_every_year']:
        UserEveryYear.objects.create(
            new_user=user['new_user']
        )


    # 每年的评论数
    for review_count in statistics['review_count_year']:
        ReviewCountYear.objects.create(
            year=review_count['year'],
            review=review_count['review']
        )

    # 统计每年的总用户数和沉默用户数
    for tas in statistics['total_and_silent']:
        TotalAndSilent.objects.create(
            year=tas['year'],
            total_users=tas['total_users'],
            reviewed_users=tas['reviewed_users'],
            silent_users=tas['silent_users'],
            silent_ratio=tas['silent_ratio'],
        )

    return JsonResponse({"message": "Update user data succeeded"})


@require_http_methods(['GET'])
def update_score_statistics(request):
    # 更新数据
    try:
        statistics = analyse.update_scores()
    except Exception as e:
        return JsonResponse({"message": f"Failed to update score data: {str(e)}"}, status=500)

    # 清空现有数据
    StarsDistribution.objects.all().delete()
    ReviewInWeek.objects.all().delete()
    Top5Businesses.objects.all().delete()

    # 评分分布（1-5）
    for star in statistics['stars_dist']:
        StarsDistribution.objects.create(
            rating=star['rating'],
            review_count=star['review_count']
        )

    # 每周各天的评分次数
    for review in statistics['review_in_week']:
        ReviewInWeek.objects.create(
            weekday_name=review['weekday_name'],
            review_count=review['review_count']
        )

    # 5星评价最多的前5个商家
    for business in statistics['top5_businesses']:
        Top5Businesses.objects.create(
            business_id=business['business_id'],
            name=business['name'],
            five_star_count=business['five_star_count']
        )

    return JsonResponse({"message": "Update score data succeeded"})



@require_http_methods(['GET'])
def init_all(request):
    analyse.clean()
    return JsonResponse({"message": "Initialization completed"})

# 在这里把一坨数据都取出来了
@require_http_methods(['GET'])
def get_statistics(request):
    statistics = {
        #
        "most_common_shop": list(MostCommonShop.objects.all().values('name', 'shop_count')),
        "shop_most_city": list(ShopMostCity.objects.all().values('city', 'shop_count')),
        "shop_most_state": list(ShopMostState.objects.all().values('state', 'shop_count')),
        "common_with_rate": list(CommonWithRate.objects.all().values('name', 'avg_stars')),
        "stars_high_city": list(StarsHighCity.objects.all().values('city', 'average_stars')),
        "most_stars": list(MostStars.objects.all().values('business_id', 'business_name', 'five_stars_counts')),
        "review_in_year": list(ReviewInYear.objects.all().values('year', 'review_count')),
        "business_checkin_ranking": list(BusinessCheckinRanking.objects.all().values('name', 'city', 'total_checkins'))[:10],
        "city_checkin_ranking": list(CityCheckinRanking.objects.all().values('city', 'total_checkins'))[:10],
        "checkin_per_hour": list(CheckinPerHour.objects.all().values('hour', 'checkin_count')),
        "checkin_per_year": list(CheckinPerYear.objects.all().values('year', 'checkin_count')),
        "elite_user_percent": list(EliteUserPercent.objects.all().values('year', 'ratio')),
        # 新的
        "new_user_every_year": list(NewUserEveryYear.objects.all().values('year', 'user_count')),
        # 统计评论达人
        "review_count": list(ReviewCount.objects.all().values('user_id', 'name', 'review_count'))[:10],
        # 统计人气最高的用户（fans）
        "fans_most": list(FanMost.objects.all().values('user_id', 'name', 'fans')),
        # 每年的新用户数
        "user_every_year": list(UserEveryYear.objects.all().values('new_user')),
        # 每年的评论数
        "review_count_year": list(ReviewCountYear.objects.all().values('review', 'year')),
        # 统计每年的总用户数和沉默用户数
        "total_and_silent": list(TotalAndSilent.objects.all().values('year', 'total_users', 'reviewed_users', 'silent_users', 'silent_ratio')),
    }

    return JsonResponse(statistics)

# 获得商户的数据
@require_http_methods(['GET'])
def get_business_statistics(request):
    statistics = {
        "most_common_shop": list(MostCommonShop.objects.all().values('name', 'shop_count')),
        "shop_most_city": list(ShopMostCity.objects.all().values('city', 'shop_count')),
        "shop_most_state": list(ShopMostState.objects.all().values('state', 'shop_count')),
        "common_with_rate": list(CommonWithRate.objects.all().values('name', 'avg_stars')),
        "stars_high_city": list(StarsHighCity.objects.all().values('city', 'average_stars')),
        "most_stars": list(MostStars.objects.all().values('business_id', 'business_name', 'five_stars_counts')),
        "review_in_year": list(ReviewInYear.objects.all().values('year', 'review_count')),
        "business_checkin_ranking": list(BusinessCheckinRanking.objects.all().values('name', 'city', 'total_checkins'))[
                                    :10],
        "city_checkin_ranking": list(CityCheckinRanking.objects.all().values('city', 'total_checkins'))[:10],
        "checkin_per_hour": list(CheckinPerHour.objects.all().values('hour', 'checkin_count')),
        "checkin_per_year": list(CheckinPerYear.objects.all().values('year', 'checkin_count')),
        "elite_user_percent": list(EliteUserPercent.objects.all().values('year', 'ratio')),
    }

    return JsonResponse(statistics)

# 获得用户的数据
@require_http_methods(['GET'])
def get_user_statistics(request):
    statistics = {
        # 新的
        "new_user_every_year": list(NewUserEveryYear.objects.all().values('year', 'user_count')),
        # 统计评论达人
        "review_count": list(ReviewCount.objects.all().values('user_id', 'name', 'review_count'))[:10],
        # 统计人气最高的用户（fans）
        "fans_most": list(FanMost.objects.all().values('user_id', 'name', 'fans')),
        # 每年的新用户数
        "user_every_year": list(UserEveryYear.objects.all().values('new_user')),
        # 每年的评论数
        "review_count_year": list(ReviewCountYear.objects.all().values('review', 'year')),
        # 统计每年的总用户数和沉默用户数
        "total_and_silent": list(
            TotalAndSilent.objects.all().values('year', 'total_users', 'reviewed_users', 'silent_users',
                                                'silent_ratio')),
    }

    return JsonResponse(statistics)

@require_http_methods(['GET'])
def get_score_statistics(request):
    statistics = {
        # 评分分布（1-5）
        "stars_dist": list(StarsDistribution.objects.all().values('rating', 'review_count')),
        # 每周各天的评分次数
        "review_in_week": list(ReviewInWeek.objects.all().values('weekday_name', 'review_count')),
        # 5星评价最多的前5个商家
        "top5_businesses": list(Top5Businesses.objects.all().values('name', 'five_star_count')),
    }

    return JsonResponse(statistics)

# 搜索和详情
@require_http_methods(['GET'])
def list_nearby_businesses(request, latitude, longitude):
    # 实现搜索逻辑
    return JsonResponse({"message": "Nearby businesses"})

@require_http_methods(['GET'])
def get_business_details(request, business_id):
    spark = get_spark_session()
    register_haversine_udf()
    competitors = find_competitors(business_id)
    attr_stats, most_common_price = calculate_attribute_distribution(competitors)
    target_df = spark.sql(f"SELECT attributes FROM business WHERE business_id = '{business_id}'")
    target_attrs = enhanced_parse_attributes(target_df.first()[0])
    comparison_dict = create_comparison_dict(attr_stats, most_common_price, target_attrs)
    spark.stop()
    return JsonResponse(comparison_dict)
    # 实现详情逻辑
    return JsonResponse({"message": "Business details"})

# 排序和筛选
@require_http_methods(['GET'])
def sort_businesses(request):
    # 实现排序逻辑
    return JsonResponse({"message": "Sorted businesses"})

@require_http_methods(['GET'])
def filter_businesses(request):
    # 实现筛选逻辑
    return JsonResponse({"message": "Filtered businesses"})

# 推荐
@require_http_methods(['GET'])
def get_review_recommendations(request, user_id):
    # 实现推荐逻辑
    return JsonResponse({"message": "Review recommendations"})

# methods for updating the statistics of the reviews
@require_http_methods(['GET'])
def update_review_statistics(request):
    # 更新数据
    try:
        statistics = update_review()
    except Exception as e:
        return JsonResponse({"message": f"Failed to update review data: {str(e)}"}, status=500)

    # 清空现有数据
    YearReviewCount.objects.all().delete()
    UserReviewCount.objects.all().delete()
    TopWord.objects.all().delete()
    GraphNode.objects.all().delete()
    GraphEdge.objects.all().delete()

    # 年度评论统计
    for year_review in statistics['year_review_counts']:
        YearReviewCount.objects.create(
            year=year_review['year'],
            review_counts=year_review['review_counts']
        )

    # 用户评论统计
    for user_review in statistics['user_review_counts']:
        UserReviewCount.objects.create(
            user_id=user_review['user_id'],
            name=user_review['name'],
            review_counts=user_review['review_counts']
        )

    # 评论高频词
    for top_word in statistics['top_20_words']:
        TopWord.objects.create(
            word=top_word['word'],
            count=top_word['count']
        )

    # 评论关系图
    for node in statistics['graph_data']['nodes']:
        GraphNode.objects.create(
            name=node['name']
        )

    for edge in statistics['graph_data']['edges']:
        source_node, _ = GraphNode.objects.get_or_create(name=edge['source'])
        target_node, _ = GraphNode.objects.get_or_create(name=edge['target'])
        GraphEdge.objects.create(
            source=source_node,
            target=target_node,
            value=edge['value']
        )

    return JsonResponse({"message": "Update review data succeeded"})


# method for getting the data of reviews
@require_http_methods(['GET'])
def get_review_statistics(request):
    statistics = {
        "year_review_counts": list(YearReviewCount.objects.all().values('year', 'review_counts')),
        "user_review_counts": list(UserReviewCount.objects.all().values('user_id', 'name', 'review_counts')),
        "top_20_words": list(TopWord.objects.all().values('word', 'count')),
        "graph_data": {
            "nodes": list(GraphNode.objects.all().values('name')),
            "edges": list(GraphEdge.objects.all().values('source__name', 'target__name', 'value')),
        },
    }

    return JsonResponse(statistics)

@require_http_methods(['GET'])
def update_checkin_statistics(request):

    # 获取数据
    try:
        statistics = update_checkin()
    except Exception as e:
        return JsonResponse({"message": f"Failed to update review data: {str(e)}"}, status=500)

    # 删表
    BusinessCheckinRanking.objects.all().delete()
    CityCheckinRanking.objects.all().delete()
    CheckinPerHour.objects.all().delete()
    CheckinPerYear.objects.all().delete()

    # 插值
    for ranking in statistics['business_checkin_ranking']:
        BusinessCheckinRanking.objects.create(
            name=ranking['name'],
            city=ranking['city'],
            total_checkins=ranking['total_checkins']
        )

    for ranking in statistics['city_checkin_ranking']:
        CityCheckinRanking.objects.create(
            city=ranking['city'],
            total_checkins=ranking['total_checkins']
        )

    for count in statistics['checkin_per_hour']:
        CheckinPerHour.objects.create(
            hour=count['hour'],
            checkin_count=count['count']
        )

    for count in statistics['checkin_per_year']:
        CheckinPerYear.objects.create(
            year=count['year'],
            checkin_count=count['count']
        )

    # 返回
    return JsonResponse({"message": "Update checkin data succeeded"})