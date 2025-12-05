import re
from rapidfuzz.distance import Levenshtein

# ---------------------------------------------------------
# NORMALIZATION FUNCTION
# Removes punctuation, extra spaces, lowercases everything,
# and standardizes legal suffixes like LLC, Inc, Co, etc.
# ---------------------------------------------------------
def normalize(name: str) -> str:
    if not name:
        return ""

    name = name.lower()

    # Remove punctuation
    name = re.sub(r"[^a-z0-9 ]+", " ", name)

    # Collapse spaces
    name = re.sub(r"\s+", " ", name).strip()

    # Standardize common business suffixes
    replacements = {
        " llc": "",
        " inc": "",
        " incorporated": "",
        " co": "",
        " company": "",
        " ltd": "",
        " limited": "",
        " corp": "",
        " corporation": "",
        " llp": "",
        " lp": "",
        " gmbh": "",
        " llc.": "",
        " inc.": "",
        " co.": "",
        " corp.": "",
    }

    for suffix, repl in replacements.items():
        if name.endswith(suffix):
            name = name[: -len(suffix)]

    return name.strip()


# ---------------------------------------------------------
# LEVENSHTEIN DISTANCE WRAPPER
# ---------------------------------------------------------
def compute_distance(a: str, b: str) -> int:
    if not a or not b:
        return 99  # treat missing data as very distant

    return Levenshtein.distance(a, b)
