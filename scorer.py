def email_score(e1,e2):
    if not e1 or not e2: return 0
    return -1 if e1.strip().lower()==e2.strip().lower() else 0

def city_score(c1,c2):
    if not c1 or not c2: return 0
    return -1 if c1.strip().lower()==c2.strip().lower() else 1


# --- Added in v1.0.4 ---
def normalize_phone(p):
    if not p:
        return None
    return ''.join(c for c in str(p) if c.isdigit())

def phone_score(p1, p2):
    n1, n2 = normalize_phone(p1), normalize_phone(p2)
    if not n1 or not n2:
        return 0
    return -1 if n1 == n2 else 0

def customer_code_score(c1, c2):
    if not c1 or not c2:
        return 0
    return -2 if str(c1).strip().upper() == str(c2).strip().upper() else 0
