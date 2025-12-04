import argparse
from pipeline import run_pipeline
from config import DEFAULT_COMPANY_CODE

def parse_args():
    parser = argparse.ArgumentParser(description="Fuzzy Match Pipeline")
    parser.add_argument("--company", default=DEFAULT_COMPANY_CODE,
                        help="Company code or ALL")
    parser.add_argument("--batch-scores", action="store_true",
                        help="Run scoring per batch")
    parser.add_argument("--final-score-only", action="store_true",
                        help="Run scoring only at end")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run_pipeline(
        company_code=args.company,
        batch_scores=args.batch_scores,
        final_scores=args.final_score_only
    )
