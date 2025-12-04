def email_score(e1,e2):
    if not e1 or not e2: return 0
    return -1 if e1.strip().lower()==e2.strip().lower() else 0

def city_score(c1,c2):
    if not c1 or not c2: return 0
    return -1 if c1.strip().lower()==c2.strip().lower() else 1
