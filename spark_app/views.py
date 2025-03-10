from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from .utils import analyse
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
    TotalAndSilent,
)

from .utils.function.distance import nearby_shop


# 初始化和统计
@require_http_methods(['GET'])
def for_test(request):
    return JsonResponse({"message": "hello from django"})

@require_http_methods(['GET'])
def update_statistics(request):
    statistics = analyse.clean()

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
    NewUserEveryYear.objects.all().delete()
    ReviewCount.objects.all().delete()
    FanMost.objects.all().delete()
    UserEveryYear.objects.all().delete()
    ReviewCountYear.objects.all().delete()
    TotalAndSilent.objects.all().delete()

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
        ReviewInYear.objects.create(year=review[0], review_count=review[1])

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
    statistics = analyse.update_business()
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
        ReviewInYear.objects.create(year=review[0], review_count=review[1])

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
    statistics = analyse.update_users()

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
        "review_count": list(ReviewCount.objects.all().values('user_id', 'name', 'review_count')),
        # 统计人气最高的用户（fans）
        "fans_most": list(FanMost.objects.all().values('user_id', 'name', 'fans')),
        # 每年的新用户数
        "user_every_year": list(UserEveryYear.objects.all().values('new_user')),
        # 每年的评论数
        "review_count_year": list(ReviewCountYear.objects.all().values('review')),
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
        "review_count_year": list(ReviewCountYear.objects.all().values('review')),
        # 统计每年的总用户数和沉默用户数
        "total_and_silent": list(
            TotalAndSilent.objects.all().values('year', 'total_users', 'reviewed_users', 'silent_users',
                                                'silent_ratio')),
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
    attr_stats.show(100, truncate=False)
    target_df = spark.sql(f"SELECT attributes FROM business WHERE business_id = '{business_id}'")
    target_attrs = enhanced_parse_attributes(target_df.first()[0])
    comparison_dict = create_comparison_dict(attr_stats, most_common_price, target_attrs)
    spark.stop()
    return JsonResponse(comparison_dict)

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


@csrf_exempt
def shop_nearby(request):
    if request.method == 'POST':
        data = json.loads(request.body)  # 解析 JSON 数据
        latitude = data.get('latitude')
        longitude = data.get('longitude')

        if latitude is None or longitude is None:
            return JsonResponse({'status': 'error', 'message': '缺少必要的参数'}, status=400)

        # 返回响应
        return nearby_shop(latitude, longitude)


    return JsonResponse({'status': 'error', 'message': '仅支持 POST 请求'}, status=405)