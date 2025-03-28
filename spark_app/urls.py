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
    get_business_statistics, get_user_statistics,
    update_user_statistics, update_business_statistics, update_score_statistics, get_score_statistics,
    update_review_statistics, get_review_statistics, update_checkin_statistics, get_checkin_statistics,
    update_wordcloud_data, get_wordcloud_data, update_restaurantCount_statistics, get_restaurantCount_statistics,
    update_wordcloud_data, get_wordcloud_data, get_business_information, get_wordcloud_data, update_yearly_statistics, get_yearly_statistics,
    update_wordcloud_data, get_wordcloud_data, update_restaurantCount_statistics, get_restaurantCount_statistics, recommend_friend

)

urlpatterns = [
    path('for_test/', for_test, name='for_test'),
    path('init_all/', init_all, name='init_all'),
    path('get_statistics/', get_statistics, name='get_statistics'),
    path('get_user_statistics/', get_user_statistics, name='get_user_statistics'),
    path('get_business_statistics/', get_business_statistics, name='get_business_statistics'),
    path('get_score_statistics/', get_score_statistics, name='get_score_statistics'),
    path('get_review_statistics/', get_review_statistics, name='get_review_statistics'),
    path('get_checkin_statistics/', get_checkin_statistics, name='get_checkin_statistics'),
    path('get_restaurantCount_statistics/', get_restaurantCount_statistics, name='get_restaurantCount_statistics'),
    path('update_statistics/', update_statistics, name='update_statistics'),
    path('update_business_statistics/', update_business_statistics, name='update_business_statistics'),
    path('update_user_statistics/', update_user_statistics, name='update_user_statistics'),
    path('update_score_statistics/', update_score_statistics, name='update_score_statistics'),
    path('update_review_statistics/', update_review_statistics, name='update_reviews_statistics'),
    path('update_checkin_statistics/', update_checkin_statistics, name='update_checkin_statistics'),
    path('update_restaurantCount_statistics/', update_restaurantCount_statistics, name='update_restaurantCount_statistics'),
    path('businesses/nearby/<str:latitude>/<str:longitude>/', list_nearby_businesses, name='list_nearby_businesses'),
    path('businesses/<str:business_id>/', get_business_details, name='get_business_details'),
    path('businesses/sort/', sort_businesses, name='sort_businesses'),
    path('businesses/filter/', filter_businesses, name='filter_businesses'),
    path('recommendations/reviews/<int:user_id>/', get_review_recommendations, name='get_review_recommendations'),
    path('update_wordcloud_data/', update_wordcloud_data, name='get_word_cloud_data'),
    path('get_wordcloud_data/', get_wordcloud_data, name='get_word_cloud_data'),
    path('get_business_information/<str:business_id>/',get_business_information, name='get_business_information' ),
    path('update_yearly_statistics/', update_yearly_statistics, name='update_yearly_statistics'),
    path('get_yearly_statistics/', get_yearly_statistics, name='get_yearly_statistics'),
    path('friend_recommend/', recommend_friend, name='recommend_friend')
]