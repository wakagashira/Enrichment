import re
def normalize_name(name):
    if not name: return ""
    name=name.lower()
    name=re.sub(r'[^a-z0-9]','',name)
    for s in ["llc","inc","co","company","corp","corporation","ltd"]:
        if name.endswith(s):
            name=name[:-len(s)]
    return name.strip()
