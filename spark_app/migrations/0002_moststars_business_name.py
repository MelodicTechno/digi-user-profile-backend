# Generated by Django 3.2.15 on 2025-03-06 09:47

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('spark_app', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='moststars',
            name='business_name',
            field=models.CharField(default='Unkonwn', max_length=255),
            preserve_default=False,
        ),
    ]
