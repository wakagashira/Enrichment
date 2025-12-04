from rapidfuzz.distance import Levenshtein
from normalizer import normalize_name

def compute_distance(a,b):
    a=normalize_name(a); b=normalize_name(b)
    if len(a)<4 or len(b)<4:
        return 0 if a==b else 4
    return Levenshtein.distance(a,b)
