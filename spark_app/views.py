from django.shortcuts import render

# Create your views here.
# spark_app/views.py
from django.http import HttpResponse
from .analyse_data import analyse_data

def analyse(request):
    analyse_data()
    return HttpResponse("Data analysis completed!")

