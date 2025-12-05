import argparse
from dotenv import load_dotenv
import os
import urllib.parse
from pipeline import run_pipeline, load_all_company_codes, get_engine
from utils import timestamp

# Load environment variables from .env
load_dotenv()

# ------------------------------------------------------
# READ FROM EXISTING .env (your original variable names)
# ------------------------------------------------------
SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")
USER = os.getenv("SQL_USER")
PASSWORD = os.getenv("SQL_PASSWORD")        # may contain special chars
COMPANY_CODE = os.getenv("COMPANY_CODE", "ALL")

# Scoring params
MAX_DIST = int(os.getenv("DIST", "5"))
BATCH_SCORES = os.getenv("RUN_SCORES_PER_BATCH", "False").lower() == "true"
FINAL_SCORE_ONLY = os.getenv("RUN_FINAL_SCORE_ONLY", "False").lower() == "true"

# ------------------------------------------------------
# SAFELY ENCODE PASSWORD FOR SQLALCHEMY URL
# (#, %, @, :, / must be encoded)
# ------------------------------------------------------
ENCODED_PASSWORD = urllib.parse.quote_plus(PASSWORD)

# ------------------------------------------------------
# BUILD SQLALCHEMY CONNECTION STRING
# ------------------------------------------------------
CONN_STR = (
    f"mssql+pyodbc://{USER}:{ENCODED_PASSWORD}@{SERVER}/{DATABASE}"
    f"?driver=ODBC+Driver+17+for+SQL+Server"
)


def main():
    print(f"[{timestamp()}] Loading SQL engine...")
    engine = get_engine(CONN_STR)

    # ------------------------------------------------------
    # Determine which company codes to run
    # ------------------------------------------------------
    if COMPANY_CODE.upper() == "ALL":
        print(f"[{timestamp()}] 🔍 Loading ALL company codes from Salesforce Partner__c ...")
        company_list = load_all_company_codes(engine)

        print(f"[{timestamp()}] Found {len(company_list)} company codes: {company_list}")

    else:
        company_list = [COMPANY_CODE]

    # ------------------------------------------------------
    # Execute pipeline for each company code
    # ------------------------------------------------------
    for code in company_list:
        print(f"\n[{timestamp()}] === Running company code: {code} ===")

        run_pipeline(
            engine=engine,
            company_code=code,
            max_dist=MAX_DIST,
            batch_scores=BATCH_SCORES,
            final_scores=FINAL_SCORE_ONLY
        )

        print(f"[{timestamp()}] === Finished company code: {code} ===\n")

    print(f"[{timestamp()}] === ALL DONE ===")


if __name__ == "__main__":
    main()
