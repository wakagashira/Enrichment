import os
import re

NORMALIZER_VERSION = os.getenv("NORMALIZER_VERSION", "v1").strip().lower()

# Try to use rapidfuzz; if unavailable, we gracefully degrade
try:
    from rapidfuzz import fuzz
    _HAS_RAPIDFUZZ = True
except Exception:
    fuzz = None
    _HAS_RAPIDFUZZ = False


######################################################################
# NORMALIZER V1 (LEGACY)
######################################################################

def normalize_v1(name: str) -> str:
    if not name:
        return ""
    # Keep this intentionally conservative to preserve legacy behavior
    return re.sub(r"\s+", " ", str(name).lower().strip())


######################################################################
# NORMALIZER V2 (AGGRESSIVE COMPANY NORMALIZATION)
######################################################################

LEGAL_SUFFIXES = {
    "inc", "incorporated", "llc", "l.l.c", "ltd", "limited", "corp", "corporation",
    "co", "company", "holdings", "holding", "group", "intl", "international"
}

STOPWORDS = {"the"}

def normalize_v2(name: str) -> str:
    if not name:
        return ""

    s = str(name).lower()

    # Normalize symbols
    s = s.replace("&", " and ")

    # Remove punctuation -> spaces
    s = re.sub(r"[^\w\s]", " ", s)

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()

    tokens = []
    for t in s.split():
        if t in STOPWORDS:
            continue
        if t in LEGAL_SUFFIXES:
            continue
        tokens.append(t)

    return " ".join(tokens)


def normalize(name: str) -> str:
    """
    Public normalize function used by pipeline.
    Controlled by env: NORMALIZER_VERSION=v1|v2
    """
    if NORMALIZER_VERSION == "v2":
        return normalize_v2(name)
    return normalize_v1(name)


######################################################################
# DISTANCE (LEGACY)
######################################################################
# NOTE: you likely already have a compute_distance implementation.
# If you currently use python-Levenshtein, keep it. Below is a safe fallback.

try:
    from Levenshtein import distance as _lev_distance
except Exception:
    _lev_distance = None

def compute_distance(a: str, b: str) -> int:
    a = a or ""
    b = b or ""
    if _lev_distance is not None:
        return int(_lev_distance(a, b))

    # Fallback Levenshtein (pure python)
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)

    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        for j, cb in enumerate(b, start=1):
            ins = curr[j - 1] + 1
            dele = prev[j] + 1
            sub = prev[j - 1] + (ca != cb)
            curr.append(min(ins, dele, sub))
        prev = curr
    return prev[-1]


######################################################################
# FUZZY SCORING (NEW)
######################################################################

def fuzzy_similarity_score(a: str, b: str) -> int:
    """
    Returns 0..100 similarity score.
    Uses token-based scorers to handle reorder/extra tokens.
    """
    if not a or not b or not _HAS_RAPIDFUZZ:
        return 0

    # Composite: take max of several robust scorers
    return int(max(
        fuzz.WRatio(a, b),
        fuzz.token_set_ratio(a, b),
        fuzz.token_sort_ratio(a, b),
        fuzz.partial_ratio(a, b),
    ))


def fuzzy_score_to_dist(score_0_100: int, max_dist: int) -> int:
    """
    Converts 0..100 similarity to a 'distance-like' integer so it can fit
    into your existing dist/max_dist filter and scoring.
    - score >= 95 -> dist 0
    - score >= 90 -> dist 1
    - score >= 85 -> dist 2
    - score >= 80 -> dist 3
    - else -> max_dist (treated as too far)
    """
    s = int(score_0_100 or 0)

    if s >= 95:
        return 0
    if s >= 90:
        return 1
    if s >= 85:
        return 2
    if s >= 80:
        return 3

    return max_dist
