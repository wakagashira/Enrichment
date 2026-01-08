import pandas as pd
from sqlalchemy import create_engine, text
from matcher import compute_distance, normalize
from utils import timestamp

######################################################################
# STATE NORMALIZATION (BI & SF STATE FIELDS → 2-CHAR CODES)
######################################################################

US_STATE_MAP = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
    "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN",
    "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY"
}


def normalize_state(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    if not s:
        return ""
    if len(s) == 2:
        return s.upper()
    return US_STATE_MAP.get(s.lower(), "")


######################################################################
# ENGINE CREATOR
######################################################################

def get_engine(conn_str: str):
    return create_engine(conn_str, fast_executemany=True)


######################################################################
# LOAD ALL COMPANY CODES
######################################################################

def load_all_company_codes(engine):
    query = """
        SELECT DISTINCT Company_Code__c
        FROM salesforce.creteProd.Partner__c
        WHERE Company_Code__c IS NOT NULL
        ORDER BY Company_Code__c;
    """
    df = pd.read_sql(text(query), engine)
    return df["Company_Code__c"].dropna().tolist()


######################################################################
# LOAD SALESFORCE ACCOUNTS
######################################################################

def load_sf_accounts(engine, company_code: str) -> pd.DataFrame:
    query = """
        SELECT
            a.Id,
            a.Name,
            a.Email__c,
            a.BillingCity,
            a.BillingState,
            a.ShippingCity,
            a.ShippingState,
            a.Spectrum_Customer_Code__c
        FROM salesforce.creteProd.Account a
        INNER JOIN salesforce.creteProd.Partner__c p
            ON a.Partner__c = p.Id
        WHERE a.Spectrum_Customer_Code__c IS NULL
          AND p.Company_Code__c = :cc;
    """
    return pd.read_sql(text(query), engine, params={"cc": company_code})


######################################################################
# LOAD BI CUSTOMERS
######################################################################

def load_bi_customers(engine, company_code: str) -> pd.DataFrame:
    query = """
        SELECT
            id AS CustomerId,
            Customer_Code AS CustomerNumber,
            Name,
            Customer_Email,
            City,
            State
        FROM dbo.Master_Customer_From_BI
        WHERE Company_Code = :cc;
    """
    return pd.read_sql(text(query), engine, params={"cc": company_code})


######################################################################
# LOAD SPECTRUM CUSTOMERS (NEW)
######################################################################

def load_spectrum_customers(engine, company_code: str) -> pd.DataFrame:
    query = """
        SELECT
            Customer_Code AS CustomerId,
            Customer_Code AS CustomerNumber,
            Name,
            Customer_Email,
            City,
            State
        FROM salesforce.Spectrum.CR_CUSTOMER_MASTER_MC
        WHERE Company_Code = :cc
          AND Status = 'A';
    """
    return pd.read_sql(text(query), engine, params={"cc": company_code})


######################################################################
# BLOCK CANDIDATE PAIRS
######################################################################

def block_pairs(df_sf: pd.DataFrame, df_bi: pd.DataFrame) -> pd.DataFrame:
    if df_sf.empty or df_bi.empty:
        return pd.DataFrame()

    df_sf = df_sf.rename(columns={
        "Id": "AccountId",
        "Name": "SFName",
        "Email__c": "SFEmail",
        "BillingCity": "SFBillingCity",
        "BillingState": "SFBillingState",
        "ShippingCity": "SFShippingCity",
        "ShippingState": "SFShippingState",
        "Spectrum_Customer_Code__c": "SpectrumCode"
    })

    df_bi = df_bi.rename(columns={
        "CustomerId": "CustomerId",
        "CustomerNumber": "CustomerNumber",
        "Name": "BIName",
        "Customer_Email": "BIEmail",
        "City": "BICity",
        "State": "BIState"
    })

    df_sf["FirstChar"] = df_sf["SFName"].str[0].str.upper()
    df_bi["FirstChar"] = df_bi["BIName"].str[0].str.upper()

    df_sf["NameLen_sf"] = df_sf["SFName"].str.len()
    df_bi["NameLen_bi"] = df_bi["BIName"].str.len()

    merged = df_bi.merge(df_sf, on="FirstChar", how="inner")

    merged = merged[
        (merged["NameLen_bi"] - merged["NameLen_sf"]).abs() <= 3
    ]

    return merged


######################################################################
# COMPUTE MATCHES (UNCHANGED)
######################################################################

def compute_matches(df_pairs: pd.DataFrame, max_dist: int) -> pd.DataFrame:
    results = []

    if df_pairs.empty:
        return pd.DataFrame()

    for _, row in df_pairs.iterrows():
        bi_name_raw = row["BIName"] or ""
        sf_name_raw = row["SFName"] or ""

        bi_norm = normalize(bi_name_raw)
        sf_norm = normalize(sf_name_raw)

        min_len = min(len(bi_norm), len(sf_norm))
        if min_len == 0:
            dist = max_dist + 1
        elif min_len < 4:
            dist = 0 if bi_norm == sf_norm else max(4, max_dist)
        else:
            dist = compute_distance(bi_norm, sf_norm)

        if dist > max_dist:
            continue

        bi_email = (row["BIEmail"] or "").strip().lower()
        sf_email = (row["SFEmail"] or "").strip().lower()
        email_score = -1 if bi_email and bi_email == sf_email else 0

        bi_city = (row["BICity"] or "").strip().lower()
        sf_cities = [
            (row.get("SFBillingCity") or "").strip().lower(),
            (row.get("SFShippingCity") or "").strip().lower()
        ]
        sf_cities_clean = [c for c in sf_cities if c]

        if not bi_city and not sf_cities_clean:
            city_score = 0
        elif bi_city and any(bi_city == c for c in sf_cities_clean):
            city_score = -1
        else:
            city_score = 1

        bi_state = normalize_state((row["BIState"] or "").strip())
        sf_states = [
            normalize_state((row.get("SFBillingState") or "").strip()),
            normalize_state((row.get("SFShippingState") or "").strip())
        ]
        sf_states_clean = [s for s in sf_states if s]

        if not bi_state and not sf_states_clean:
            state_score = 0
        elif bi_state and any(bi_state == s for s in sf_states_clean):
            state_score = -1
        else:
            state_score = 1

        total_score = dist + email_score + city_score + state_score

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
            "SalesforceCityBilling": row["SFBillingCity"],
            "SalesforceCityShipping": row["SFShippingCity"],
            "BuildOpsState": row["BIState"],
            "SalesforceStateBilling": row["SFBillingState"],
            "SalesforceStateShipping": row["SFShippingState"],
            "Dist": dist,
            "EmailScore": email_score,
            "AddressCityScore": city_score,
            "StateScore": state_score,
            "TotalScore": total_score
        })

    df = pd.DataFrame(results)
    if df.empty:
        return df

    df["BestMatchFlag"] = df.groupby("CustomerId")["TotalScore"].transform(
        lambda s: (s == s.min()).astype(int)
    )

    return df


######################################################################
# INSERT RESULTS (UNCHANGED)
######################################################################

def insert_results(engine, df_results: pd.DataFrame) -> int:
    if df_results.empty:
        return 0

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
        "SalesforceCityBilling",
        "SalesforceCityShipping",
        "BuildOpsState",
        "SalesforceStateBilling",
        "SalesforceStateShipping",
        "Dist",
        "EmailScore",
        "AddressCityScore",
        "StateScore",
        "TotalScore",
        "BestMatchFlag"
    ]

    df_results[cols].to_sql(
        "ResultsBI",
        engine,
        if_exists="append",
        index=False
    )

    return len(df_results)


######################################################################
# MAIN PIPELINE (PATCHED)
######################################################################

def run_pipeline(
    engine,
    company_code: str,
    max_dist: int,
    source_system: str = "BUILDOPS",
    batch_scores: bool = True,
    final_scores: bool = False
):
    print(f"[{timestamp()}] Loading Salesforce accounts for {company_code}")
    df_sf = load_sf_accounts(engine, company_code)
    print(f"[{timestamp()}] Loaded {len(df_sf)} Salesforce accounts")

    if source_system == "SPECTRUM":
        print(f"[{timestamp()}] Loading Spectrum customers for {company_code}")
        df_bi = load_spectrum_customers(engine, company_code)
    else:
        print(f"[{timestamp()}] Loading BI customers for {company_code}")
        df_bi = load_bi_customers(engine, company_code)

    print(f"[{timestamp()}] Loaded {len(df_bi)} customers")

    print(f"[{timestamp()}] Blocking candidate pairs...")
    df_pairs = block_pairs(df_sf, df_bi)
    print(f"[{timestamp()}] Generated {len(df_pairs)} candidate pairs")

    if df_pairs.empty:
        print(f"[{timestamp()}] No candidate pairs for {company_code}, skipping.")
        return

    df_pairs["CompanyCode"] = company_code

    print(f"[{timestamp()}] Computing matches (max_dist={max_dist})...")
    df_results = compute_matches(df_pairs, max_dist=max_dist)
    print(f"[{timestamp()}] Produced {len(df_results)} scored matches")

    if df_results.empty:
        return

    print(f"[{timestamp()}] Inserting results into ResultsBI...")
    inserted = insert_results(engine, df_results)
    print(f"[{timestamp()}] Inserted {inserted} rows into ResultsBI for {company_code}")
