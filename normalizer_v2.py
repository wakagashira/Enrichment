import re

LEGAL_SUFFIXES = {
    "inc", "incorporated", "llc", "l.l.c", "ltd", "limited",
    "corp", "corporation", "gmbh", "plc", "co", "company",
    "holdings", "group", "international"
}

STOPWORDS = {"the", "and", "&"}

def _clean_company_name(name: str) -> str:
    if not name:
        return ""

    name = name.lower()

    # replace symbols
    name = name.replace("&", " and ")

    # remove punctuation
    name = re.sub(r"[^\w\s]", " ", name)

    tokens = []
    for token in name.split():
        if token in STOPWORDS:
            continue
        if token in LEGAL_SUFFIXES:
            continue
        tokens.append(token)

    return " ".join(tokens)

def _acronym(name: str) -> str:
    return "".join(word[0] for word in name.split() if len(word) > 2)

def normalize(data):
    normalized = []

    for record in data:
        record = record.copy()

        company = record.get("company_name", "")
        cleaned = _clean_company_name(company)

        record["company_name_original"] = company
        record["company_name_normalized"] = cleaned
        record["company_name_acronym"] = _acronym(cleaned)

        # normalize all other fields conservatively
        for k, v in record.items():
            if isinstance(v, str):
                record[k] = v.strip().lower()

        normalized.append(record)

    return normalized
