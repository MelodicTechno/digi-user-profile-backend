# Generated by Django 3.2.25 on 2025-03-11 13:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('spark_app', '0007_americanreviewcount_americanreviewstars_chinesereviewcount_chinesereviewstars_mexicanreviewcount_mex'),
    ]

    operations = [
        migrations.CreateModel(
            name='RestaurantsReviewCount',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('type', models.CharField(max_length=255)),
                ('count', models.IntegerField()),
            ],
        ),
        migrations.DeleteModel(
            name='AmericanReviewCount',
        ),
        migrations.DeleteModel(
            name='AmericanReviewStars',
        ),
        migrations.DeleteModel(
            name='ChineseReviewCount',
        ),
        migrations.DeleteModel(
            name='ChineseReviewStars',
        ),
        migrations.DeleteModel(
            name='MexicanReviewCount',
        ),
        migrations.DeleteModel(
            name='MexicanReviewStars',
        ),
    ]
