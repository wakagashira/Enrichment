import pandas as pd
from db import get_connection
from utils.logger import log
from matcher import compute_distance
from scorer import email_score, city_score
from config import MAX_DIST


def load_sf_accounts(conn, company_code):
    query = """
        SELECT 
            a.Id,
            a.Name,
            a.Email__c,
            a.Spectrum_Customer_Code__c,
            a.BillingCity
        FROM salesforce.creteProd.Account a
        INNER JOIN salesforce.creteProd.Partner__c p
            ON a.Partner__c = p.Id
        WHERE a.Spectrum_Customer_Code__c IS NULL
          AND (p.Company_Code__c = ? OR ? IS NULL)
    """
    return pd.read_sql(query, conn, params=[company_code, company_code])


def load_bi_customers(conn, company_code):
    query = """
        SELECT 
            id AS CustomerId,
            Customer_Code AS CustomerNumber,
            Name,
            Customer_Email,
            City
        FROM dbo.Master_Customer_From_BI
        WHERE Company_Code = ? OR ? IS NULL
    """
    return pd.read_sql(query, conn, params=[company_code, company_code])


def block_pairs(df_customers, df_accounts):
    # Add blocking features
    df_customers["FirstLetter"] = df_customers["CustomerName"].str[:1].str.lower() \
        if "CustomerName" in df_customers else df_customers["Name"].str[:1].str.lower()

    df_accounts["FirstLetter"] = df_accounts["Name"].str[:1].str.lower()

    df_customers["NameLen"] = df_customers["CustomerName"].str.len() \
        if "CustomerName" in df_customers else df_customers["Name"].str.len()

    df_accounts["NameLen"] = df_accounts["Name"].str.len()

    # Rename columns to avoid suffix issues
    df_accounts = df_accounts.rename(columns={
        "Id": "AccountId",
        "Name": "AccountName",
        "Email__c": "AccountEmail",
        "BillingCity": "AccountCity"
    })

    df_customers = df_customers.rename(columns={
        "Name": "CustomerName",
        "Customer_Email": "CustomerEmail",
        "City": "CustomerCity"
    })

    # Merge using FirstLetter blocker
    merged = df_customers.merge(
        df_accounts,
        on="FirstLetter",
        how="inner"
    )

    # Keep only similar-length names
    return merged[
        abs(merged["CustomerName"].str.len() - merged["AccountName"].str.len()) <= 3
    ]



def compute_matches(df):
    rows = []

    for _, r in df.iterrows():
        dist = compute_distance(r["CustomerName"], r["AccountName"])
        em = email_score(r["CustomerEmail"], r["AccountEmail"])
        cs = city_score(r["CustomerCity"], r["AccountCity"])
        total = dist + em + cs

        if dist <= MAX_DIST:
            rows.append({
                "CustomerId": r["CustomerId"],
                "CustomerNumber": r["CustomerNumber"],
                "AccountId": r["AccountId"],
                "BuildOpsName": r["CustomerName"],
                "SalesforceName": r["AccountName"],
                "Dist": dist,
                "EmailScore": em,
                "AddressCityScore": cs,
                "TotalScore": total
            })

    df_res = pd.DataFrame(rows)

    if df_res.empty:
        return df_res

    df_res["BestMatchFlag"] = df_res.groupby("CustomerId")["TotalScore"] \
                                     .transform(lambda x: x == x.min())

    return df_res


def insert_results(cursor, conn, df, company_code):
    if df.empty:
        return 0

    sql = """
        INSERT INTO BuildOps.dbo.ResultsBI
        (CompanyCode, CustomerId, CustomerNumber, AccountId, 
         BuildOpsName, SalesforceName, Dist, EmailScore, 
         AddressCityScore, TotalScore, BestMatchFlag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        cursor.execute(sql, (
            company_code,
            row.CustomerId,
            row.CustomerNumber,
            row.AccountId,
            row.BuildOpsName,
            row.SalesforceName,
            row.Dist,
            row.EmailScore,
            row.AddressCityScore,
            row.TotalScore,
            int(row.BestMatchFlag)
        ))

    conn.commit()
    return len(df)


def run_pipeline(company_code="ALL", batch_scores=True, final_scores=False):
    conn = get_connection()
    cursor = conn.cursor()

    log(f"=== Running company code: {company_code} ===")

    # Load data
    df_accounts = load_sf_accounts(conn, company_code)
    df_customers = load_bi_customers(conn, company_code)

    log(f"Loaded {len(df_accounts)} Salesforce accounts")
    log(f"Loaded {len(df_customers)} BI customers")

    # Blocking
    df_pairs = block_pairs(df_customers, df_accounts)
    log(f"Generated {len(df_pairs)} candidate pairs")

    # Compute fuzzy matches
    df_results = compute_matches(df_pairs)
    log(f"Produced {len(df_results)} matches (<= MAX_DIST={MAX_DIST})")

    # Insert results
    inserted = insert_results(cursor, conn, df_results, company_code)
    log(f"Inserted {inserted} rows into ResultsBI")

    conn.close()
    log("=== Done ===")
