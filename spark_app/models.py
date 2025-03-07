from django.db import models

class MostCommonShop(models.Model):
    name = models.CharField(max_length=255)
    shop_count = models.IntegerField()

    def __str__(self):
        return self.name

class ShopMostCity(models.Model):
    city = models.CharField(max_length=255)
    shop_count = models.IntegerField()

    def __str__(self):
        return self.city

class ShopMostState(models.Model):
    state = models.CharField(max_length=255)
    shop_count = models.IntegerField()

    def __str__(self):
        return self.state

class CommonWithRate(models.Model):
    name = models.CharField(max_length=255)
    avg_stars = models.FloatField()

    def __str__(self):
        return self.name

class StarsHighCity(models.Model):
    city = models.CharField(max_length=255)
    average_stars = models.FloatField()

    def __str__(self):
        return self.city

class MostStars(models.Model):
    business_id = models.CharField(max_length=255)
    business_name = models.CharField(max_length=255)
    five_stars_counts = models.IntegerField()

    def __str__(self):
        return self.business_id

class ReviewInYear(models.Model):
    year = models.IntegerField()
    review_count = models.IntegerField()

    def __str__(self):
        return str(self.year)

class BusinessCheckinRanking(models.Model):
    name = models.CharField(max_length=255)
    city = models.CharField(max_length=255)
    total_checkins = models.IntegerField()

    def __str__(self):
        return f"{self.name} in {self.city}"

class CityCheckinRanking(models.Model):
    city = models.CharField(max_length=255)
    total_checkins = models.IntegerField()

    def __str__(self):
        return self.city

class CheckinPerHour(models.Model):
    hour = models.IntegerField()
    checkin_count = models.IntegerField()

    def __str__(self):
        return str(self.hour)

class CheckinPerYear(models.Model):
    year = models.IntegerField()
    checkin_count = models.IntegerField()

    def __str__(self):
        return str(self.year)

class EliteUserPercent(models.Model):
    year = models.IntegerField()
    ratio = models.FloatField()

    def __str__(self):
        return str(self.year)

