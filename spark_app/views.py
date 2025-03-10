import json

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
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
    EliteUserPercent
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
        MostStars.objects.create(business_id=stars[0], five_stars_counts=stars[1])

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

    return JsonResponse({"message": "Update data succeeded"})

@require_http_methods(['GET'])
def init_all(request):
    analyse.clean()
    return JsonResponse({"message": "Initialization completed"})

@require_http_methods(['GET'])
def get_statistics(request):
    statistics = {
        "most_common_shop": list(MostCommonShop.objects.all().values('name', 'shop_count')),
        "shop_most_city": list(ShopMostCity.objects.all().values('city', 'shop_count')),
        "shop_most_state": list(ShopMostState.objects.all().values('state', 'shop_count')),
        "common_with_rate": list(CommonWithRate.objects.all().values('name', 'avg_stars')),
        "stars_high_city": list(StarsHighCity.objects.all().values('city', 'average_stars')),
        "most_stars": list(MostStars.objects.all().values('business_id', 'five_stars_counts')),
        "review_in_year": list(ReviewInYear.objects.all().values('year', 'review_count')),
        "business_checkin_ranking": list(BusinessCheckinRanking.objects.all().values('name', 'city', 'total_checkins')),
        "city_checkin_ranking": list(CityCheckinRanking.objects.all().values('city', 'total_checkins')),
        "checkin_per_hour": list(CheckinPerHour.objects.all().values('hour', 'checkin_count')),
        "checkin_per_year": list(CheckinPerYear.objects.all().values('year', 'checkin_count')),
        "elite_user_percent": list(EliteUserPercent.objects.all().values('year', 'ratio')),
    }

    return JsonResponse(statistics)

# 搜索和详情
@require_http_methods(['GET'])
def list_nearby_businesses(request, latitude, longitude):
    # 实现搜索逻辑
    return JsonResponse({"message": "Nearby businesses"})

@require_http_methods(['GET'])
def get_business_details(request, business_id):
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