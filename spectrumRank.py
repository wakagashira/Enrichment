from sqlalchemy import create_engine
from config import CONNECTION_STRING, MAX_DIST
from pipeline import run_pipeline, load_all_company_codes
from utils import timestamp

def main():
    print(f"[{timestamp()}] Starting Spectrum ranking job")

    engine = create_engine(CONNECTION_STRING)

    company_codes = load_all_company_codes(engine)

    for cc in company_codes:
        print(f"[{timestamp()}] Processing company {cc}")
        run_pipeline(
            engine=engine,
            company_code=cc,
            max_dist=MAX_DIST,
            source_system="SPECTRUM"
        )

    print(f"[{timestamp()}] Spectrum ranking completed")

if __name__ == "__main__":
    main()
