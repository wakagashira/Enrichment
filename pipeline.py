import pandas as pd
from sqlalchemy import create_engine, text
from matcher import compute_distance, normalize
from utils import timestamp


######################################################################
# ENGINE CREATOR
######################################################################
def get_engine(conn_str: str):
    """
    Create a SQLAlchemy engine from a connection string.
    """
    return create_engine(conn_str, fast_executemany=True)


######################################################################
# LOAD ALL COMPANY CODES (USED WHEN COMPANY_CODE=ALL)
######################################################################
def load_all_company_codes(engine):
    """
    Load all distinct Company_Code__c values from Salesforce Partner__c.
    """
    query = """
        SELECT DISTINCT Company_Code__c
        FROM salesforce.creteProd.Partner__c
        WHERE Company_Code__c IS NOT NULL
        ORDER BY Company_Code__c;
    """
    df = pd.read_sql(text(query), engine)
    return df["Company_Code__c"].dropna().tolist()


######################################################################
# LOAD SALESFORCE ACCOUNTS FOR ONE COMPANY
######################################################################
def load_sf_accounts(engine, company_code: str) -> pd.DataFrame:
    """
    Load Salesforce accounts for a given Company_Code__c.
    Filters to accounts with NULL Spectrum_Customer_Code__c.
    """
    query = """
        SELECT 
            a.Id,
            a.Name,
            a.Email__c,
            a.BillingCity,
            a.Spectrum_Customer_Code__c
        FROM salesforce.creteProd.Account a
        INNER JOIN salesforce.creteProd.Partner__c p
            ON a.Partner__c = p.Id
        WHERE a.Spectrum_Customer_Code__c IS NULL
          AND p.Company_Code__c = :cc;
    """
    return pd.read_sql(text(query), engine, params={"cc": company_code})


######################################################################
# LOAD BI CUSTOMERS FOR ONE COMPANY
######################################################################
def load_bi_customers(engine, company_code: str) -> pd.DataFrame:
    """
    Load BI customers from Master_Customer_From_BI for a given Company_Code.
    """
    query = """
        SELECT
            id AS CustomerId,
            Customer_Code AS CustomerNumber,
            Name,
            Customer_Email,
            City
        FROM dbo.Master_Customer_From_BI
        WHERE Company_Code = :cc;
    """
    return pd.read_sql(text(query), engine, params={"cc": company_code})


######################################################################
# BLOCK CANDIDATE PAIRS
######################################################################
def block_pairs(df_sf: pd.DataFrame, df_bi: pd.DataFrame) -> pd.DataFrame:
    """
    Blocking step to reduce the Cartesian product:
    - Merge on first character of name
    - Filter by name length band (±3)
    Returns a DataFrame with BI and SF columns clearly renamed.
    """
    if df_sf.empty or df_bi.empty:
        return pd.DataFrame()

    # Rename Salesforce columns to stable names
    df_sf = df_sf.rename(columns={
        "Id": "AccountId",
        "Name": "SFName",
        "Email__c": "SFEmail",
        "BillingCity": "SFCity",
        "Spectrum_Customer_Code__c": "SpectrumCode"
    })

    # Rename BI columns to stable names
    df_bi = df_bi.rename(columns={
        "CustomerId": "CustomerId",
        "CustomerNumber": "CustomerNumber",
        "Name": "BIName",
        "Customer_Email": "BIEmail",
        "City": "BICity"
    })

    # Blocking keys
    df_sf["FirstChar"] = df_sf["SFName"].str[0].str.upper()
    df_bi["FirstChar"] = df_bi["BIName"].str[0].str.upper()

    df_sf["NameLen_sf"] = df_sf["SFName"].str.len()
    df_bi["NameLen_bi"] = df_bi["BIName"].str.len()

    # Merge on FirstChar
    merged = df_bi.merge(
        df_sf,
        on="FirstChar",
        how="inner"
    )

    # Length window filter
    merged = merged[
        (merged["NameLen_bi"] - merged["NameLen_sf"]).abs() <= 3
    ]

    return merged


######################################################################
# COMPUTE MATCHES (NAME DIST + EMAIL + CITY SCORE)
######################################################################
def compute_matches(df_pairs: pd.DataFrame, max_dist: int) -> pd.DataFrame:
    """
    Compute fuzzy matches + email and city scores.
    - Uses normalize() from matcher.py
    - Short-name rule: if normalized length < 4, require exact match or treat as dist=4
    - EmailScore: 0 if no data or mismatch, -1 if exact match
    - AddressCityScore: 0 if missing, -1 if match, +1 if mismatch
    - TotalScore = dist + EmailScore + AddressCityScore
    """
    results = []

    if df_pairs.empty:
        return pd.DataFrame()

    for _, row in df_pairs.iterrows():
        # Normalized names
        bi_name_raw = row["BIName"] or ""
        sf_name_raw = row["SFName"] or ""

        bi_norm = normalize(bi_name_raw)
        sf_norm = normalize(sf_name_raw)

        # Short-name rule
        min_len = min(len(bi_norm), len(sf_norm))
        if min_len == 0:
            # No usable name; treat as very poor match
            dist = max_dist + 1
        elif min_len < 4:
            if bi_norm == sf_norm:
                dist = 0
            else:
                # Treat as poor match (at least 4)
                dist = max(4, max_dist)
        else:
            dist = compute_distance(bi_norm, sf_norm)

        if dist > max_dist:
            continue

        # Email score
        bi_email = (row["BIEmail"] or "").strip().lower()
        sf_email = (row["SFEmail"] or "").strip().lower()

        if not bi_email or not sf_email:
            email_score = 0
        elif bi_email == sf_email:
            email_score = -1
        else:
            email_score = 0  # neutral mismatch

        # City score
        bi_city = (row["BICity"] or "").strip().lower()
        sf_city = (row["SFCity"] or "").strip().lower()

        if not bi_city or not sf_city:
            city_score = 0
        elif bi_city == sf_city:
            city_score = -1
        else:
            city_score = 1

        total_score = dist + email_score + city_score

        results.append({
            "CompanyCode": row["CompanyCode"],
            "CustomerId": row["CustomerId"],
            "CustomerNumber": row["CustomerNumber"],
            "AccountId": row["AccountId"],
            "Spectrum_Customer_Code__c": row.get("SpectrumCode"),
            "BuildOpsName": bi_name_raw,
            "SalesforceName": sf_name_raw,
            "BuildOpsEmail": row["BIEmail"],
            "SalesforceEmail": row["SFEmail"],
            "BuildOpsCity": row["BICity"],
            "SalesforceCity": row["SFCity"],
            "Dist": dist,
            "EmailScore": email_score,
            "AddressCityScore": city_score,
            "TotalScore": total_score
        })

    df = pd.DataFrame(results)

    if df.empty:
        return df

    # Best match per CustomerId (lowest TotalScore)
    df["BestMatchFlag"] = df.groupby("CustomerId")["TotalScore"].transform(
        lambda s: (s == s.min()).astype(int)
    )

    return df


######################################################################
# INSERT INTO ResultsBI
######################################################################
def insert_results(engine, df_results: pd.DataFrame) -> int:
    """
    Append match results into BuildOps.dbo.ResultsBI.
    Expects ResultsBI to have matching columns.
    """
    if df_results.empty:
        return 0

    # Order columns for clarity (must match or be subset of table schema)
    cols = [
        "CompanyCode",
        "CustomerId",
        "CustomerNumber",
        "AccountId",
        "Spectrum_Customer_Code__c",
        "BuildOpsName",
        "SalesforceName",
        "BuildOpsEmail",
        "SalesforceEmail",
        "BuildOpsCity",
        "SalesforceCity",
        "Dist",
        "EmailScore",
        "AddressCityScore",
        "TotalScore",
        "BestMatchFlag"
    ]

    df_to_insert = df_results[cols]

    df_to_insert.to_sql(
        "ResultsBI",
        engine,
        if_exists="append",
        index=False
    )

    return len(df_to_insert)


######################################################################
# MAIN PIPELINE PER COMPANY CODE
######################################################################
def run_pipeline(engine,
                 company_code: str,
                 max_dist: int,
                 batch_scores: bool = True,
                 final_scores: bool = False):
    """
    Main pipeline:
    - Load SF accounts for company_code
    - Load BI master customers for company_code
    - Block into candidate pairs
    - Compute fuzzy matches and scores
    - Insert into ResultsBI
    """
    print(f"[{timestamp()}] Loading Salesforce accounts for {company_code}")
    df_sf = load_sf_accounts(engine, company_code)
    print(f"[{timestamp()}] Loaded {len(df_sf)} Salesforce accounts")

    print(f"[{timestamp()}] Loading BI customers for {company_code}")
    df_bi = load_bi_customers(engine, company_code)
    print(f"[{timestamp()}] Loaded {len(df_bi)} BI customers")

    print(f"[{timestamp()}] Blocking candidate pairs...")
    df_pairs = block_pairs(df_sf, df_bi)
    print(f"[{timestamp()}] Generated {len(df_pairs)} candidate pairs")

    if df_pairs.empty:
        print(f"[{timestamp()}] No candidate pairs for {company_code}, skipping.")
        return

    # Tag company code on each row
    df_pairs["CompanyCode"] = company_code

    print(f"[{timestamp()}] Computing matches (max_dist={max_dist})...")
    df_results = compute_matches(df_pairs, max_dist=max_dist)
    print(f"[{timestamp()}] Produced {len(df_results)} scored matches")

    if df_results.empty:
        print(f"[{timestamp()}] No matches survived scoring for {company_code}.")
        return

    print(f"[{timestamp()}] Inserting results into ResultsBI...")
    inserted = insert_results(engine, df_results)
    print(f"[{timestamp()}] Inserted {inserted} rows into ResultsBI for {company_code}")
