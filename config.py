import os
from dotenv import load_dotenv
load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

DEFAULT_COMPANY_CODE = os.getenv("COMPANY_CODE", "ALL")

RUN_SCORES_PER_BATCH = os.getenv("RUN_SCORES_PER_BATCH", "True") == "True"
RUN_FINAL_SCORE_ONLY = os.getenv("RUN_FINAL_SCORE_ONLY", "False") == "True"

# NEW: Maximum allowed Levenshtein distance
MAX_DIST = int(os.getenv("DIST", "10"))
