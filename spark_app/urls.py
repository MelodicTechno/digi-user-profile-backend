from django.urls import path
from .views import (
    for_test,
    init_all,
    get_statistics,
    list_nearby_businesses,
    get_business_details,
    sort_businesses,
    filter_businesses,
    get_review_recommendations, update_statistics,
)

urlpatterns = [
    path('for_test/', for_test, name='for_test'),
    path('init_all/', init_all, name='init_all'),
    path('get_statistics/', get_statistics, name='get_statistics'),
    path('update_statistics/', update_statistics, name='update_statistics'),
    path('businesses/nearby/<str:latitude>/<str:longitude>/', list_nearby_businesses, name='list_nearby_businesses'),
    path('businesses/<int:business_id>/', get_business_details, name='get_business_details'),
    path('businesses/sort/', sort_businesses, name='sort_businesses'),
    path('businesses/filter/', filter_businesses, name='filter_businesses'),
    path('recommendations/reviews/<int:user_id>/', get_review_recommendations, name='get_review_recommendations'),
]