# Generated by Django 3.2.15 on 2025-03-10 01:47

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('spark_app', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='fanmost',
            name='user_id',
            field=models.CharField(max_length=255),
        ),
        migrations.AlterField(
            model_name='reviewcount',
            name='user_id',
            field=models.CharField(max_length=255),
        ),
    ]
