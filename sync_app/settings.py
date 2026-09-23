import os
import secrets
from pathlib import Path
from urllib.parse import urlsplit

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("AD_ORG_SYNC_DATA_DIR", ".appdata")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEBUG = os.environ.get("DJANGO_DEBUG") == "1"
key_file = DATA_DIR / "django-secret-key"
if not os.environ.get("DJANGO_SECRET_KEY") and not key_file.exists():
    try:
        fd = os.open(key_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(secrets.token_urlsafe(64))
    except FileExistsError:
        pass
SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY") or key_file.read_text().strip()
PUBLIC_URL = os.environ.get("AD_ORG_SYNC_PUBLIC_BASE_URL", "http://127.0.0.1:8010").rstrip("/")
ALLOWED_HOSTS = [urlsplit(PUBLIC_URL).hostname or "localhost", "localhost", "127.0.0.1", "testserver"]
CSRF_TRUSTED_ORIGINS = [PUBLIC_URL]
INSTALLED_APPS = ["django.contrib.admin", "django.contrib.auth", "django.contrib.contenttypes", "django.contrib.sessions", "django.contrib.messages", "django.contrib.staticfiles", "sync_app.apps.SyncAppConfig"]
MIDDLEWARE = ["django.middleware.security.SecurityMiddleware", "whitenoise.middleware.WhiteNoiseMiddleware", "django.contrib.sessions.middleware.SessionMiddleware", "django.middleware.common.CommonMiddleware", "django.middleware.csrf.CsrfViewMiddleware", "django.contrib.auth.middleware.AuthenticationMiddleware", "django.contrib.messages.middleware.MessageMiddleware", "django.middleware.clickjacking.XFrameOptionsMiddleware"]
ROOT_URLCONF = "sync_app.urls"
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates", "APP_DIRS": True, "OPTIONS": {"context_processors": ["django.template.context_processors.request", "django.contrib.auth.context_processors.auth", "django.contrib.messages.context_processors.messages"]}}]
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": DATA_DIR / "django.sqlite3", "OPTIONS": {"timeout": 20, "transaction_mode": "IMMEDIATE"}}}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
LANGUAGE_CODE = "zh-hans"
TIME_ZONE = "Asia/Shanghai"
USE_TZ = True
STATIC_URL = "/static/"
STATIC_ROOT = DATA_DIR / "static"
LOGIN_URL = "/login"
LOGIN_REDIRECT_URL = "/dashboard"
LOGOUT_REDIRECT_URL = "/login"
SESSION_COOKIE_SECURE = PUBLIC_URL.startswith("https://")
CSRF_COOKIE_SECURE = SESSION_COOKIE_SECURE
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Strict"
SESSION_COOKIE_AGE = 3600
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_HSTS_SECONDS = 31536000 if SESSION_COOKIE_SECURE else 0
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True
X_FRAME_OPTIONS = "DENY"
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
AUTH_PASSWORD_VALIDATORS = [{"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator", "OPTIONS": {"min_length": 12}}, {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"}]
DATA_UPLOAD_MAX_MEMORY_SIZE = 65536
DINGTALK_CORP_ID = os.environ.get("DINGTALK_CORP_ID", "")
DINGTALK_APP_KEY = os.environ.get("DINGTALK_APP_KEY", "")
DINGTALK_APP_SECRET = os.environ.get("DINGTALK_APP_SECRET", "")
SSPR_ALLOWED_DINGTALK_USER_IDS = frozenset(
    value.strip() for value in os.environ.get("SSPR_ALLOWED_DINGTALK_USER_IDS", "").split(",") if value.strip()
)
LDAP_HOST = os.environ.get("LDAP_HOST", "")
LDAP_BIND_DN = os.environ.get("LDAP_BIND_DN", "")
LDAP_PASSWORD = os.environ.get("LDAP_PASSWORD", "")
LDAP_BASE_DN = os.environ.get("LDAP_BASE_DN", "")
LDAP_VERIFY_CERT = os.environ.get("LDAP_VERIFY_CERT", "false").strip().lower() not in {"0", "false", "no", "off"}
LDAP_CA_FILE = os.environ.get("LDAP_CA_FILE") or ("/run/secrets/ad-ca.pem" if Path("/run/secrets/ad-ca.pem").is_file() else None)
