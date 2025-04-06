import pandas as pd

battery_chem_map = {
    1: "Lithium-Ion Batteries",
    2: "Nickel-Metal Hydride Batteries",
    3: "Lead-Acid Batteries",
    4: "Ultracapacitors",
    5: "Other"
}

charger_type_map = {
    "1": "Level 1",
    "2": "Level 2",
    "3": "DCFC",
    "4": "Other"
}

connector_type_map = {
    "1": "J1772",
    "2": "CCS connector",
    "3": "CHAdeMO connector",
    "4": "Tesla connector",
    "5": "Mennekes connector",
    "6": "Other",
}

def map_multi_labels(series, mapping_dict):
    return series.fillna("").apply(
        lambda x: ", ".join(
            [mapping_dict.get(i.strip(), i.strip()) for i in x.split(",") if i.strip()]
        )
    )

def format_if_notna(value, placeholder="N/A"):
    """Return original value if not null; otherwise return placeholder."""
    if pd.notna(value):
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        return value
    else:
        return placeholder
