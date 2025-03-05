# spark_app/urls.py
from django.urls import path
from .views import init_all

urlpatterns = [
    path('analyse/init/', init_all(), name='init_all'),
]
