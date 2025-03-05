from rest_framework import generics
from .models import Business
from .serializers import BusinessSerializer
from .models import Review
from .serializers import ReviewSerializer
from rest_framework.views import APIView
from rest_framework.response import Response
from pyspark.sql import SparkSession
from .utils import analyse

class BusinessList(generics.ListAPIView):
    queryset = Business.objects.all()
    serializer_class = BusinessSerializer

class BusinessDetail(generics.RetrieveAPIView):
    queryset = Business.objects.all()
    serializer_class = BusinessSerializer

class ReviewList(generics.ListAPIView):
    queryset = Review.objects.all()
    serializer_class = ReviewSerializer

class ReviewDetail(generics.RetrieveAPIView):
    queryset = Review.objects.all()
    serializer_class = ReviewSerializer



# 初始化和统计
class ForTest(APIView):
    def get(self, request):
        return Response({"message": "hello from django"})
class InitAllView(APIView):
    def get(self, request):
        analyse.clean()
        return Response({"message": "Initialization completed"})

class GetStatisticsView(APIView):
    def get(self, request):
        statistics = analyse.clean()
        return Response(statistics)

# 搜索和详情
class ListNearbyBusinessesView(APIView):
    def get(self, request, latitude, longitude):
        # 实现搜索逻辑
        return Response({"message": "Nearby businesses"})

class GetBusinessDetailsView(APIView):
    def get(self, request, business_id):
        # 实现详情逻辑
        return Response({"message": "Business details"})

# 排序和筛选
class SortBusinessesView(APIView):
    def get(self, request):
        # 实现排序逻辑
        return Response({"message": "Sorted businesses"})

class FilterBusinessesView(APIView):
    def get(self, request):
        # 实现筛选逻辑
        return Response({"message": "Filtered businesses"})

# 推荐
class GetReviewRecommendationsView(APIView):
    def get(self, request, user_id):
        # 实现推荐逻辑
        return Response({"message": "Review recommendations"})