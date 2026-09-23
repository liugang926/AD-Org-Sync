from django.contrib import admin
from django.contrib.auth.views import LogoutView
from django.urls import path
from . import views

urlpatterns = [
    path("", views.dashboard), path("dashboard", views.dashboard, name="dashboard"),
    path("login", views.AdminLogin.as_view(), name="login"),
    path("logout", LogoutView.as_view(), name="logout"),
    path("admin/login/", views.AdminLogin.as_view()),
    path("admin/", admin.site.urls),
    path("healthz", views.health), path("readyz", views.ready),
    path("people", views.people, name="people"),
    path("people/<int:person_id>", views.person_action, name="person_action"),
    path("jobs/<uuid:job_id>", views.job_detail, name="job"),
    path("logs", views.logs, name="logs"),
    path("connections/test", views.test_connections, name="test_connections"),
    path("sspr", views.employee, name="sspr"),
    path("sspr/auth/dingtalk", views.employee_auth),
    path("sspr/reset", views.employee_reset),
    path("sspr/callback/dingtalk", views.employee),
    path("sspr/oauth/start", views.employee),
]
