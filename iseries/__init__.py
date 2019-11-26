from django.core.exceptions import ImproperlyConfigured

__version__ = "1.0.0.0"

try:
    import pyodbc as Database
except ImportError as e:
    raise ImportError(
        "pyodbc module not found. Install pyodbc module with pip install pyodbc. Error: %s" % e)

version = tuple(int(x) for x in Database.version.split('.'))
if version < (4, 0, 0):
    raise ImproperlyConfigured(f'PyODBC 4.0 or later required; you have {version}.')
