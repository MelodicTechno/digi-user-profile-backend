from django.db import models

# Create your models here.
from django.db import models

from django.db import models


class AnalysisResult(models.Model):
    column_name = models.CharField(max_length=255)
    count = models.IntegerField()

    def __str__(self):
        return f"{self.column_name}: {self.count}"
