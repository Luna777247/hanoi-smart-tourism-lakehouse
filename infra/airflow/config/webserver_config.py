"""
Airflow Webserver Configuration - Basic Database Authentication
"""
import os
from flask_appbuilder.security.manager import AUTH_DB

# Allow insecure transport for development
os.environ['AUTHLIB_INSECURE_TRANSPORT'] = 'true'

# Set authentication type to Basic DB (username + password)
AUTH_TYPE = AUTH_DB

# User registration
AUTH_USER_REGISTRATION = False
AUTH_ROLE_ADMIN = 'Admin'
AUTH_ROLE_PUBLIC = 'Public'

# Additional Flask App Builder configurations
CSRF_ENABLED = True
WTF_CSRF_ENABLED = True

# Session configuration
PERMANENT_SESSION_LIFETIME = 3600  # 1 hour

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_REDIS_URL': 'redis://redis:6379/1',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'airflow_',
}

# Rate limiting
RATELIMIT_ENABLED = True
RATELIMIT_STORAGE_URL = 'redis://redis:6379/2'
