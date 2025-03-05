# 序列化器
from rest_framework import serializers
from .models import Business, Review

# Business序列化器
class BusinessSerializer(serializers.ModelSerializer):
    class Meta:
        model = Business
        fields = ['id', 'name', 'address', 'city', 'state', 'postal_code', 'stars', 'review_count']

# Review序列化器
class ReviewSerializer(serializers.ModelSerializer):
    class Meta:
        model = Review
        fields = ['id', 'business_id', 'user_id', 'stars', 'date', 'text']