import pandas as pd
from sqlalchemy import create_engine, text

from matcher import (
    compute_distance,
    normalize,
    fuzzy_similarity_score,
    fuzzy_score_to_dist,
)

from utils import timestamp

PIPELINE_VERSION = "1.0.7"

######################################################################
# STATE NORMALIZATION
######################################################################

US_STATE_MAP = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL",
    "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY"
}

def normalize_state(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    if len(s) == 2:
        return s.upper()
    return US_STATE_MAP.get(s.lower(), "")

######################################################################
# HELPERS
######################################################################

def normalize_phone(p):
    if not p:
        return ""
    digits = "".join(c for c in str(p) if c.isdigit())
    return digits[-10:] if len(digits) >= 7 else ""

def normalize_zip(z):
    if not z:
        return ""
    digits = "".join(c for c in str(z) if c.isdigit())
    return digits[:5] if len(digits) >= 5 else ""

def email_domain(e):
    if not e or "@" not in e:
        return ""
    return e.split("@", 1)[1].lower().strip()

######################################################################
# ENGINE
######################################################################

def get_engine(conn_str: str):
    return create_engine(conn_str, fast_executemany=True)

######################################################################
# LOADERS
######################################################################

def load_sf_accounts(engine, company_code: str) -> pd.DataFrame:
    query = """
        SELECT
            a.Id,
            a.Name,
            a.Email__c,
            a.Phone,
            a.BillingCity,
            a.BillingState,
            a.BillingPostalCode,
            a.ShippingCity,
            a.ShippingState,
            a.ShippingPostalCode,
            a.Spectrum_Customer_Code__c
        FROM salesforce.creteProd.Account a
        INNER JOIN salesforce.creteProd.Partner__c p
            ON a.Partner__c = p.Id
        WHERE a.Spectrum_Customer_Code__c IS NULL
          AND p.Company_Code__c = :cc;
    """
    return pd.read_sql(text(query), engine, params={"cc": company_code})


def load_spectrum_customers(engine, company_code: str) -> pd.DataFrame:
    query = """
        SELECT
            Customer_Code AS CustomerId,
            Customer_Code AS CustomerNumber,
            Name,
            Customer_Email,
            Phone,
            City,
            State,
            Zip_Code AS Zip
        FROM salesforce.Spectrum.CR_CUSTOMER_MASTER_MC
        WHERE Company_Code = :cc
          AND Status = 'A';
    """
    return pd.read_sql(text(query), engine, params={"cc": company_code})


def load_buildops_customers(engine, company_code: str) -> pd.DataFrame:
    query = """
        SELECT
            id AS CustomerId,

            CASE
                WHEN CHARINDEX('/', LTRIM(RTRIM(accountingRefId))) > 0
                THEN SUBSTRING(
                    LTRIM(RTRIM(accountingRefId)),
                    CHARINDEX('/', LTRIM(RTRIM(accountingRefId))) + 1,
                    LEN(LTRIM(RTRIM(accountingRefId)))
                )
                ELSE LTRIM(RTRIM(accountingRefId))
            END AS CustomerNumber,

            COALESCE(NULLIF(LTRIM(RTRIM(name)), ''), '[UNKNOWN]') AS Name,
            email AS Customer_Email,
            COALESCE(phonePrimary, phoneAlternate) AS Phone,
            address0_city AS City,
            address0_state AS State,
            address0_zipcode AS Zip
        FROM BuildOps.dbo.Customers
        WHERE
            accountingRefId IS NOT NULL
            AND LEN(accountingRefId) >= 4
            AND UPPER(LEFT(LTRIM(RTRIM(accountingRefId)), 3)) = :cc
            AND isActive = 1;
    """
    return pd.read_sql(text(query), engine, params={"cc": company_code})

######################################################################
# BLOCKING
######################################################################

def block_pairs(df_sf: pd.DataFrame, df_src: pd.DataFrame) -> pd.DataFrame:
    if df_sf.empty or df_src.empty:
        return pd.DataFrame()

    df_sf = df_sf.rename(columns={
        "Id": "AccountId",
        "Name": "SFName",
        "Email__c": "SFEmail",
        "Phone": "SFPhone",
        "BillingCity": "SFBillingCity",
        "BillingState": "SFBillingState",
        "BillingPostalCode": "SFBillingPostalCode",
        "ShippingCity": "SFShippingCity",
        "ShippingState": "SFShippingState",
        "ShippingPostalCode": "SFShippingPostalCode",
        "Spectrum_Customer_Code__c": "SpectrumCode"
    })

    df_src = df_src.rename(columns={
        "Name": "BIName",
        "Customer_Email": "BIEmail",
        "Phone": "BIPhone",
        "City": "BICity",
        "State": "BIState",
        "Zip": "BIZip"
    })

    df_sf["FirstChar"] = df_sf["SFName"].str[0].str.upper()
    df_src["FirstChar"] = df_src["BIName"].str[0].str.upper()

    df_sf["NameLen_sf"] = df_sf["SFName"].str.len()
    df_src["NameLen_bi"] = df_src["BIName"].str.len()

    merged = df_src.merge(df_sf, on="FirstChar", how="inner")
    return merged[(merged["NameLen_bi"] - merged["NameLen_sf"]).abs() <= 3]

######################################################################
# COMPANY CODES
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
# MATCHING
######################################################################

def compute_matches(df_pairs: pd.DataFrame, max_dist: int) -> pd.DataFrame:
    results = []

    for _, row in df_pairs.iterrows():
        bi_norm = normalize(row["BIName"] or "")
        sf_norm = normalize(row["SFName"] or "")

        min_len = min(len(bi_norm), len(sf_norm))
        if min_len == 0:
            continue

        if min_len < 4:
            legacy_dist = 0 if bi_norm == sf_norm else max_dist
        else:
            legacy_dist = compute_distance(bi_norm, sf_norm)

        fuzzy_score = fuzzy_similarity_score(bi_norm, sf_norm)
        fuzzy_dist = fuzzy_score_to_dist(fuzzy_score, max_dist)

        dist = min(legacy_dist, fuzzy_dist)
        if dist > max_dist:
            continue

        email_score = 0
        bi_email = (row.get("BIEmail") or "").lower().strip()
        sf_email = (row.get("SFEmail") or "").lower().strip()
        if bi_email and sf_email:
            if bi_email == sf_email or email_domain(bi_email) == email_domain(sf_email):
                email_score = -1

        phone_score = -2 if (
            normalize_phone(row.get("BIPhone")) and
            normalize_phone(row.get("BIPhone")) == normalize_phone(row.get("SFPhone"))
        ) else 0

        bi_city = (row.get("BICity") or "").lower().strip()
        sf_cities = [
            (row.get("SFBillingCity") or "").lower().strip(),
            (row.get("SFShippingCity") or "").lower().strip(),
        ]
        sf_cities = [c for c in sf_cities if c]

        if bi_city and bi_city in sf_cities:
            city_score = -1
        elif bi_city and sf_cities:
            city_score = 1
        else:
            city_score = 0

        zip_score = -1 if normalize_zip(row.get("BIZip")) in [
            normalize_zip(row.get("SFBillingPostalCode")),
            normalize_zip(row.get("SFShippingPostalCode"))
        ] and normalize_zip(row.get("BIZip")) else 0

        bi_state = normalize_state(row.get("BIState"))
        sf_states = [
            normalize_state(row.get("SFBillingState")),
            normalize_state(row.get("SFShippingState")),
        ]
        sf_states = [s for s in sf_states if s]

        if bi_state and bi_state in sf_states:
            state_score = -1
        elif bi_state and sf_states:
            state_score = 1
        else:
            state_score = 0

        strong_signals = sum([
            dist <= 1,
            email_score < 0,
            phone_score < 0,
            zip_score < 0,
            city_score < 0,
            state_score < 0
        ])

        multi_signal_bonus = -1 if strong_signals >= 3 else 0
        total_score = dist + email_score + phone_score + zip_score + city_score + state_score + multi_signal_bonus

        confidence = "HIGH" if total_score <= 0 else "MEDIUM" if total_score <= 2 else "LOW"

        results.append({
            "CompanyCode": row["CompanyCode"],

            "BuildOpsName": row.get("BIName"),
            "BuildOpsEmail": row.get("BIEmail"),
            "BuildOpsCity": row.get("BICity"),
            "BuildOpsState": row.get("BIState"),

            "SalesforceName": row.get("SFName"),
            "SalesforceEmail": row.get("SFEmail"),
            "SalesforceCityBilling": row.get("SFBillingCity"),
            "SalesforceCityShipping": row.get("SFShippingCity"),
            "SalesforceStateBilling": row.get("SFBillingState"),
            "SalesforceStateShipping": row.get("SFShippingState"),

            # Business-facing customer identifier
            "CustomerId": (
                row["CustomerNumber"]
                if row.get("SourceSystem", "BUILDOPS") == "BUILDOPS"
                else row["CustomerId"]
            ),

            # Optional: keep CustomerNumber for traceability
            "CustomerNumber": row["CustomerNumber"],
            "AccountId": row["AccountId"],
            "Spectrum_Customer_Code__c": row.get("SpectrumCode"),

            "Dist": dist,
            "EmailScore": email_score,
            "PhoneScore": phone_score,
            "ZipScore": zip_score,
            "AddressCityScore": city_score,
            "StateScore": state_score,
            "MultiSignalBonus": multi_signal_bonus,
            "TotalScore": total_score,
            "ConfidenceBand": confidence
        })

    df = pd.DataFrame(results)
    if not df.empty:
        df["BestMatchFlag"] = df.groupby("CustomerId")["TotalScore"].transform(
            lambda s: (s == s.min()).astype(int)
        )
    return df

######################################################################
# INSERT
######################################################################

def insert_results(engine, df: pd.DataFrame) -> int:
    df["RunDate"] = timestamp()
    df.to_sql("ResultsBI", engine, if_exists="append", index=False)
    return len(df)

######################################################################
# MAIN
######################################################################

def run_pipeline(
    engine,
    company_code: str,
    max_dist: int,
    source_system="BUILDOPS",
    batch_scores=False,
    final_scores=False,
):
    df_sf = load_sf_accounts(engine, company_code)

    if source_system == "BUILDOPS":
        df_src = load_buildops_customers(engine, company_code)
    elif source_system == "SPECTRUM":
        df_src = load_spectrum_customers(engine, company_code)
    else:
        raise ValueError(f"Unknown source_system: {source_system}")

    if df_sf.empty or df_src.empty:
        return

    df_pairs = block_pairs(df_sf, df_src)
    if df_pairs.empty:
        return

    df_pairs["CompanyCode"] = company_code
    df_results = compute_matches(df_pairs, max_dist)
    if df_results.empty:
        return

    df_results["SourceSystem"] = source_system
    insert_results(engine, df_results)
