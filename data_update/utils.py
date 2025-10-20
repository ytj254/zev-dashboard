import pandas as pd
import numpy as np

def to_boolean(series: pd.Series) -> pd.Series:
    """
    Convert Yes/No, Y/N, True/False, 1/0 (case/space-insensitive) to nullable booleans.
    Leaves anything else as <NA>. Safe for StringArray.
    """
    vals = series.astype("string").str.strip().str.lower()
    mapping = {
        "y": True, "yes": True, "true": True, "t": True, "1": True,
        "n": False, "no": False, "false": False, "f": False, "0": False,
        "": np.nan, "nan": np.nan, "none": np.nan
    }
    out = vals.map(mapping)
    return out.astype("boolean")