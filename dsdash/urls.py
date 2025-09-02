from django.contrib import admin
from django.urls import path
from api import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.index, name='index'),
    path('api/dataset/upload', views.upload_dataset, name='upload_dataset'),
    path('api/metrics/schema', views.schema, name='schema'),
    path('api/metrics/nulls', views.nulls, name='nulls'),
    path('api/metrics/duplicates', views.duplicates, name='duplicates'),
    path('api/metrics/categorical/<str:col>/top', views.categorical_top, name='categorical_top'),
    path('api/metrics/numeric/<str:col>/hist', views.numeric_hist, name='numeric_hist'),
    path('api/metrics/datetime/<str:col>/counts', views.datetime_counts, name='datetime_counts'),
    path('api/descripcion/resumen', views.descripcion_resumen, name='descripcion_resumen'),
]
