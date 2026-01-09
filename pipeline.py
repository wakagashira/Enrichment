import pandas as pd
from sqlalchemy import create_engine, text
from matcher import compute_distance, normalize
from utils import timestamp

######################################################################
# STATE NORMALIZATION
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
    if len(s) == 2:
        return s.upper()
    return US_STATE_MAP.get(s.lower(), "")

######################################################################
# NORMALIZATION HELPERS
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


def load_bi_customers(engine, company_code: str) -> pd.DataFrame:
    query = """
        SELECT
            id AS CustomerId,
            Customer_Code AS CustomerNumber,
            Name,
            Customer_Email,
            Phone,
            City,
            State,
            Zip
        FROM dbo.Master_Customer_From_BI
        WHERE Company_Code = :cc;
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

######################################################################
# BLOCKING
######################################################################

def block_pairs(df_sf: pd.DataFrame, df_bi: pd.DataFrame) -> pd.DataFrame:
    if df_sf.empty or df_bi.empty:
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

    df_bi = df_bi.rename(columns={
        "CustomerId": "CustomerId",
        "CustomerNumber": "CustomerNumber",
        "Name": "BIName",
        "Customer_Email": "BIEmail",
        "Phone": "BIPhone",
        "City": "BICity",
        "State": "BIState",
        "Zip": "BIZip"
    })

    df_sf["FirstChar"] = df_sf["SFName"].str[0].str.upper()
    df_bi["FirstChar"] = df_bi["BIName"].str[0].str.upper()

    df_sf["NameLen_sf"] = df_sf["SFName"].str.len()
    df_bi["NameLen_bi"] = df_bi["BIName"].str.len()

    merged = df_bi.merge(df_sf, on="FirstChar", how="inner")
    return merged[(merged["NameLen_bi"] - merged["NameLen_sf"]).abs() <= 3]
######################################################################
# LOAD ALL COMPANY CODES (USED WHEN COMPANY_CODE = ALL)
######################################################################

def load_all_company_codes(engine):
    """
    Load all distinct Company_Code__c values from Salesforce Partner__c.
    Used by spectrumrank.py when COMPANY_CODE=ALL.
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
# SCORING + BONUS + CONFIDENCE
######################################################################

def compute_matches(df_pairs: pd.DataFrame, max_dist: int) -> pd.DataFrame:
    results = []

    for _, row in df_pairs.iterrows():
        bi_norm = normalize(row["BIName"] or "")
        sf_norm = normalize(row["SFName"] or "")

        min_len = min(len(bi_norm), len(sf_norm))
        if min_len == 0:
            continue
        elif min_len < 4:
            dist = 0 if bi_norm == sf_norm else max(4, max_dist)
        else:
            dist = compute_distance(bi_norm, sf_norm)

        if dist > max_dist:
            continue

        # Email
        email_score = 0
        bi_email = (row["BIEmail"] or "").lower().strip()
        sf_email = (row["SFEmail"] or "").lower().strip()
        if bi_email and sf_email:
            if bi_email == sf_email:
                email_score = -1
            elif email_domain(bi_email) == email_domain(sf_email):
                email_score = -1

        # Phone
        phone_score = -2 if normalize_phone(row.get("BIPhone")) == normalize_phone(row.get("SFPhone")) and normalize_phone(row.get("BIPhone")) else 0

        # City
        bi_city = (row.get("BICity") or "").lower().strip()
        sf_cities = [(row.get("SFBillingCity") or "").lower().strip(),
                     (row.get("SFShippingCity") or "").lower().strip()]
        sf_cities = [c for c in sf_cities if c]

        if bi_city and bi_city in sf_cities:
            city_score = -1
        elif bi_city and sf_cities:
            city_score = 1
        else:
            city_score = 0

        # ZIP
        zip_score = -1 if normalize_zip(row.get("BIZip")) in [
            normalize_zip(row.get("SFBillingPostalCode")),
            normalize_zip(row.get("SFShippingPostalCode"))
        ] and normalize_zip(row.get("BIZip")) else 0

        # State
        bi_state = normalize_state(row.get("BIState"))
        sf_states = [normalize_state(row.get("SFBillingState")),
                     normalize_state(row.get("SFShippingState"))]
        sf_states = [s for s in sf_states if s]

        if bi_state and bi_state in sf_states:
            state_score = -1
        elif bi_state and sf_states:
            state_score = 1
        else:
            state_score = 0

        # Multi-signal bonus
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

        # Confidence band
        if total_score <= 0:
            confidence = "HIGH"
        elif total_score <= 2:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        results.append({
            "CompanyCode": row["CompanyCode"],
            "CustomerId": row["CustomerId"],
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
    cols = [
        "CompanyCode",
        "SourceSystem",
        "CustomerId",
        "CustomerNumber",
        "AccountId",
        "Spectrum_Customer_Code__c",
        "Dist",
        "EmailScore",
        "PhoneScore",
        "ZipScore",
        "AddressCityScore",
        "StateScore",
        "MultiSignalBonus",
        "TotalScore",
        "ConfidenceBand",
        "BestMatchFlag"
    ]
    df[cols].to_sql("ResultsBI", engine, if_exists="append", index=False)
    return len(df)

######################################################################
# MAIN
######################################################################

def run_pipeline(engine, company_code: str, max_dist: int, source_system="BUILDOPS"):
    df_sf = load_sf_accounts(engine, company_code)
    df_bi = load_spectrum_customers(engine, company_code) if source_system == "SPECTRUM" else load_bi_customers(engine, company_code)

    df_pairs = block_pairs(df_sf, df_bi)
    if df_pairs.empty:
        return

    df_pairs["CompanyCode"] = company_code
    df_results = compute_matches(df_pairs, max_dist)
    if df_results.empty:
        return

    df_results["SourceSystem"] = source_system
    insert_results(engine, df_results)
