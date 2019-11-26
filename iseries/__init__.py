__version__ = "1.0.0.0"

# Importing IBM_DB wrapper ibm_db_dbi
try:
    import pyodbc as Database
except ImportError as e:
    raise ImportError(
        "pyodbc module not found. Install pyodbc module with pip install pyodbc. Error: %s" % e)
