# Generated by Django 3.2.15 on 2025-03-10 07:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('spark_app', '0002_auto_20250310_0947'),
    ]

    operations = [
        migrations.CreateModel(
            name='ReviewInWeek',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('weekday_name', models.CharField(max_length=10)),
                ('review_count', models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name='StarsDistribution',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('rating', models.IntegerField()),
                ('review_count', models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name='TipsPerYear',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('year', models.IntegerField()),
                ('tip_count', models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name='Top5Businesses',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('business_id', models.CharField(max_length=255)),
                ('five_star_count', models.IntegerField()),
            ],
        ),
        migrations.AddField(
            model_name='reviewcountyear',
            name='year',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
