from django.urls import path

from BTSApp import views

urlpatterns = [
    path('',views.index, name='index')
]
