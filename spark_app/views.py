from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from .utils import analyse
from django.core.cache import cache

# 初始化和统计
@require_http_methods(['GET'])
def for_test(request):
    return JsonResponse({"message": "hello from django"})

@require_http_methods(['GET'])
def init_all(request):
    analyse.clean()
    return JsonResponse({"message": "Initialization completed"})

@require_http_methods(['GET'])
def get_statistics(request):
    statistics = cache.get('statistics')
    if not statistics:
        statistics = analyse.clean()
        cache.set('statistics', statistics, timeout=3600)  # 缓存1小时
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