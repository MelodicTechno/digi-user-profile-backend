from django.shortcuts import render

# Create your views here.
# spark_app/views.py
from django.http import HttpResponse
from .tasks import init_all

def init_all(request):
    init_all()
    return HttpResponse("Data analysis completed!")

