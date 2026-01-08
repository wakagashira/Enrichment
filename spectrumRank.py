
import pandas as pd
from sqlalchemy import create_engine
from pipeline import run_pipeline
from config import DB_CONN_STRING
from utils import timestamp

def load_spectrum_customers(engine):
    sql = '''
        SELECT
            Company_Code        AS CompanyCode,
            Customer_Code       AS CustomerCode,
            Name                AS CustomerName,
            City,
            State,
            Zip_Code            AS Zip,
            Phone,
            Customer_Email      AS Email
        FROM Spectrum.CR_CUSTOMER_MASTER_MC
        WHERE Status = 'A'
    '''
    return pd.read_sql(sql, engine)

def main():
    print(f"[{timestamp()}] Starting Spectrum ranking job")
    engine = create_engine(DB_CONN_STRING)
    df = load_spectrum_customers(engine)

    run_pipeline(
        engine=engine,
        df_source=df,
        source_system="SPECTRUM",
        max_dist=6
    )

    print(f"[{timestamp()}] Spectrum ranking completed")

if __name__ == "__main__":
    main()
