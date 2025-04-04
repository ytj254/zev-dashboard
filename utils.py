import pandas as pd

def format_if_notna(value, placeholder="N/A"):
    """Return original value if not null; otherwise return placeholder."""
    if pd.notna(value):
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        return value
    else:
        return placeholder
