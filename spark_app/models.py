from django.db import models

"""
查询后的表 存在MySQL里
"""

# 最常见商户
class MostCommonShop(models.Model):
    name = models.CharField(max_length=255)
    shop_count = models.IntegerField()

    def __str__(self):
        return self.name

# 商户最多的城市
class ShopMostCity(models.Model):
    city = models.CharField(max_length=255)
    shop_count = models.IntegerField()

    def __str__(self):
        return self.city

# 商户最多的前5个州
class ShopMostState(models.Model):
    state = models.CharField(max_length=255)
    shop_count = models.IntegerField()

    def __str__(self):
        return self.state

# 最常见商户及其平均评分
class CommonWithRate(models.Model):
    name = models.CharField(max_length=255)
    avg_stars = models.FloatField()

    def __str__(self):
        return self.name

# 评分最高的城市
class StarsHighCity(models.Model):
    city = models.CharField(max_length=255)
    average_stars = models.FloatField()

    def __str__(self):
        return self.city

# 收获五星最多的商户
class MostStars(models.Model):
    business_id = models.CharField(max_length=255)
    business_name = models.CharField(max_length=255)
    five_stars_counts = models.IntegerField()

    def __str__(self):
        return self.business_name

# 每年评论数
class ReviewInYear(models.Model):
    year = models.IntegerField(null=True)
    review_count = models.IntegerField(null=True)

    def __str__(self):
        return str(self.year)

# 商家打卡数排序
class BusinessCheckinRanking(models.Model):
    name = models.CharField(max_length=255)
    city = models.CharField(max_length=255)
    total_checkins = models.IntegerField()

    def __str__(self):
        return f"{self.name} in {self.city}"

# 喜欢打卡的城市
class CityCheckinRanking(models.Model):
    city = models.CharField(max_length=255)
    total_checkins = models.IntegerField()

    def __str__(self):
        return self.city

# 每小时打卡数
class CheckinPerHour(models.Model):
    hour = models.IntegerField()
    checkin_count = models.IntegerField()

    def __str__(self):
        return str(self.hour)

# 每年打卡数
class CheckinPerYear(models.Model):
    year = models.IntegerField()
    checkin_count = models.IntegerField()

    def __str__(self):
        return str(self.year)

# 精英用户比
class EliteUserPercent(models.Model):
    year = models.IntegerField()
    ratio = models.FloatField()

    def __str__(self):
        return str(self.year)

# 新加入的
# 分析每年加入的用户数量
class NewUserEveryYear(models.Model):
    year = models.IntegerField()
    user_count = models.IntegerField()

    def __str__(self):
        return str(self.year)

# 统计评论达人
class ReviewCount(models.Model):
    user_id = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    review_count = models.IntegerField()

    def __str__(self):
        return str(self.user_id)


# 统计人气最高的用户（fans）
class FanMost(models.Model):
    user_id = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    fans = models.IntegerField()

    def __str__(self):
        return str(self.name)

# 每年的新用户数
class UserEveryYear(models.Model):
    new_user = models.IntegerField()

    def __str__(self):
        return str(self.new_user)

# 每年的评论数
class ReviewCountYear(models.Model):
    year = models.IntegerField()
    review = models.IntegerField()

    def __str__(self):
        return str(self.review)

# 统计每年的总用户数和沉默用户数
class TotalAndSilent(models.Model):
    year = models.IntegerField()
    total_users = models.IntegerField()
    reviewed_users = models.IntegerField()
    silent_users = models.IntegerField()
    silent_ratio = models.FloatField()

    def __str__(self):
        return f'year:{self.year} total_users:{self.total_users}'