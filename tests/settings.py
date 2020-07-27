import os
import random
import string

SECRET_KEY = [random.choice(string.ascii_lowercase) for i in range(10)]

DATABASES = {
    'default':
        {
            'ENGINE': 'django_iseries',
            'NAME': 'iseries',  # arbitrary database name for db2 on iseries
            'HOST': os.environ['TEST_SYSTEM_HOST'],
            'USER': os.environ['TEST_SYSTEM_USERNAME'],
            'PASSWORD': os.environ['TEST_SYSTEM_PASSWORD'],
            'CURRENTSCHEMA': os.environ['TEST_SYSTEM_SCHEMA'],
        },
    'other':
        {
            'ENGINE': 'django.db.backends.sqlite3',
        }
}

# Use a fast hasher to speed up tests.
PASSWORD_HASHERS = [
    'django.contrib.auth.hashers.MD5PasswordHasher',
]

INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'django.contrib.auth',
    'django.contrib.sites',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.admin.apps.SimpleAdminConfig',
    'django.contrib.staticfiles',

    'tests',
]

MIGRATION_MODULES = {
    # This lets us skip creating migrations for the test models as many of
    # them depend on one of the following contrib applications.
    'auth': None,
    'contenttypes': None,
    'sessions': None,
}
