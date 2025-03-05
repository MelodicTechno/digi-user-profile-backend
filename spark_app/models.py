from django.db import models

# Create your models here.
from django.db import models

from django.db import models


class AnalysisResult(models.Model):
    column_name = models.CharField(max_length=255)
    count = models.IntegerField()

    def __str__(self):
        return f"{self.column_name}: {self.count}"

# 商业信息
class Business(models.Model):
    name = models.CharField(max_length=255)
    address = models.CharField(max_length=255)
    city = models.CharField(max_length=100)
    state = models.CharField(max_length=100)
    postal_code = models.CharField(max_length=20)
    stars = models.FloatField()
    review_count = models.IntegerField()

    def __str__(self):
        return self.name

# 用户评价
class Review(models.Model):
    business = models.ForeignKey(Business, related_name='reviews', on_delete=models.CASCADE)
    user_id = models.CharField(max_length=255)
    stars = models.IntegerField()
    date = models.DateField()
    text = models.TextField()

    def __str__(self):
        return f"Review by {self.user_id} on {self.date}"