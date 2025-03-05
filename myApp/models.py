from django.db import models

# Create your models here.
class User(models.Model):
    id = models.AutoField("id", primary_key=True)
    username = models.CharField("username", max_length=255, default='')
    password = models.CharField("username", max_length=255, default='')

    class Meta:
        db_table = "user"