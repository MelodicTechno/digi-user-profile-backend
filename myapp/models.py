from django.db import models

# Create your models here.
from django.db import models

class SimpleUser(models.Model):
    uid = models.CharField(max_length=255)
    level = models.CharField(max_length=255)

    def __str__(self):
        return str(self.id)