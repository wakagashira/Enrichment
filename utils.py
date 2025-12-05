from datetime import datetime

def timestamp() -> str:
    """Return a clean timestamp for console logging."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
