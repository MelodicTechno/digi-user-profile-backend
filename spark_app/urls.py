# spark_app/urls.py
from django.urls import path
from .views import (
    InitAllView,
    GetStatisticsView,
    ListNearbyBusinessesView,
    GetBusinessDetailsView,
    SortBusinessesView,
    FilterBusinessesView,
    GetReviewRecommendationsView,
    ForTest,
)


urlpatterns = [
    path('init_all/', InitAllView.as_view(), name='init_all'),
    path('get_statistics/', GetStatisticsView.as_view(), name='get_statistics'),
    path('businesses/nearby/<str:latitude>/<str:longitude>/', ListNearbyBusinessesView.as_view(), name='list_nearby_businesses'),
    path('businesses/<int:business_id>/', GetBusinessDetailsView.as_view(), name='get_business_details'),
    path('businesses/sort/', SortBusinessesView.as_view(), name='sort_businesses'),
    path('businesses/filter/', FilterBusinessesView.as_view(), name='filter_businesses'),
    path('recommendations/reviews/<int:user_id>/', GetReviewRecommendationsView.as_view(), name='get_review_recommendations'),
    path('test/', ForTest.as_view(), name='test'),
]
