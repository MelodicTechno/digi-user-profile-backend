# 序列化器
from rest_framework import serializers

class YelpReviewSerializer(serializers.Serializer):
    review_id = serializers.CharField()
    user_id = serializers.CharField()
    business_id = serializers.CharField()
    stars = serializers.FloatField()
    text = serializers.CharField()