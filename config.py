import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

#Set normalizer version
NORMALIZER_VERSION = os.getenv("NORMALIZER_VERSION", "v1")

# SQL credentials
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

# Company code control
DEFAULT_COMPANY_CODE = os.getenv("COMPANY_CODE", "ALL")

# Pipeline flags
RUN_SCORES_PER_BATCH = os.getenv("RUN_SCORES_PER_BATCH", "True") == "True"
RUN_FINAL_SCORE_ONLY = os.getenv("RUN_FINAL_SCORE_ONLY", "False") == "True"

# Maximum fuzzy distance
MAX_DIST = int(os.getenv("DIST", "5"))

# Build SQLAlchemy connection string (pyodbc)
CONNECTION_STRING = (
    f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

