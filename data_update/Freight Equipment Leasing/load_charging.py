import pandas as pd
import psycopg2.extras as extras
from data_update.common_data_update import engine

# --- Config ---
VALID_CHARGERS = {f"C03P{i}" for i in range(1, 9)}  # C03P1 ... C03P8
EXCEL_FILE = "Charge Log - 2025-07-30 11_58_31.xlsx"

# --- Helpers ---
def normalize_charger(charger_str: str) -> str | None:
    """Turn 'C03, Sae J1772 Combo United States, 1' → 'C03P1'"""
    parts = [p.strip() for p in charger_str.split(",")]
    if len(parts) >= 2:
        return f"{parts[0]}P{parts[-1]}"
    return None

def fetch_valid_vehicle_ids():
    """Get all fleet_vehicle_id from DB"""
    with engine.connect() as conn:
        df = pd.read_sql("SELECT fleet_vehicle_id FROM vehicle", conn)
    return set(df["fleet_vehicle_id"].dropna().astype(str))

# --- Load Excel ---
df = pd.read_excel(EXCEL_FILE)

# Expected columns: "Charger", "Start date", "End date", "Total charging time", "Total kWh", "Linked license plate"
df["charger_id"] = df["Charger"].apply(normalize_charger)
df = df[df["charger_id"].isin(VALID_CHARGERS)]

# Keep only known vehicles
valid_veh_ids = fetch_valid_vehicle_ids()
df["veh_id"] = df["Linked license plate"].astype(str)
df = df[df["veh_id"].isin(valid_veh_ids)]

# Map DB columns
df_db = pd.DataFrame({
    "charger_id": df["charger_id"],
    "veh_id": df["veh_id"],
    "connect_time": pd.to_datetime(df["Start date"], errors="coerce"),
    "disconnect_time": pd.to_datetime(df["End date"], errors="coerce"),
    "tot_ref_dura": pd.to_numeric(df["Total charging time"], errors="coerce"),
    "tot_energy": pd.to_numeric(df["Total kWh"], errors="coerce")
})

# Compute avg_power safely
df_db["avg_power"] = (df_db["tot_energy"] * 60 / df_db["tot_ref_dura"]).round(2)
df_db["max_power"] = None
df_db["refuel_start"] = None
df_db["refuel_end"] = None

# --- Insert into DB ---
insert_sql = """
INSERT INTO public.refuel_inf (
    charger_id, veh_id, connect_time, disconnect_time,
    refuel_start, refuel_end,
    avg_power, max_power, tot_energy, tot_ref_dura
)
VALUES %s
ON CONFLICT DO NOTHING;
"""

records = df_db.to_records(index=False)
tuples = [tuple(r) for r in records]

cols = ["charger_id", "veh_id", "connect_time", "disconnect_time",
        "refuel_start", "refuel_end",
        "avg_power", "max_power", "tot_energy", "tot_ref_dura"]

with engine.raw_connection() as conn:
    with conn.cursor() as cur:
        extras.execute_values(cur, insert_sql, tuples, template=None, page_size=1000)
    conn.commit()

print(f"Inserted {len(tuples)} rows into refuel_inf")
