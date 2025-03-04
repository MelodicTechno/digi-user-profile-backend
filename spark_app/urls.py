# spark_app/urls.py
from django.urls import path
from .views import analyse

urlpatterns = [
    path('analyse/', analyse, name='analyse'),
]
