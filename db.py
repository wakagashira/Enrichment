import pyodbc
from config import SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD

def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD}"
    )
