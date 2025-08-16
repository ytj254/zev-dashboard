import pandas as pd
import psycopg2.extras as extras
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine

# --- Config ---
VALID_CHARGERS = {f"C03P{i}" for i in range(1, 9)}  # C03P1 ... C03P8
EXCEL_FILE = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Freight Equipment Leasing\Charging log\Charge Log - 2025-07-30 11_58_31.xlsx"

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
df = pd.read_excel(EXCEL_FILE, header=1)
print(df)

# # Normalize charger IDs
# df["charger_id"] = df["Charger"].apply(normalize_charger)
# df = df[df["charger_id"].isin(VALID_CHARGERS)]

# # Keep only known vehicles
# valid_veh_ids = fetch_valid_vehicle_ids()
# df["veh_id"] = df["Linked license plate"].astype(str)
# df = df[df["veh_id"].isin(valid_veh_ids)]

# # Map DB columns
# df_db = pd.DataFrame({
#     "charger_id": df["charger_id"],
#     "veh_id": df["veh_id"],
#     "connect_time": pd.to_datetime(df["Start date"], errors="coerce"),
#     "disconnect_time": pd.to_datetime(df["End date"], errors="coerce"),
#     "tot_ref_dura": pd.to_numeric(df["Total charging time"], errors="coerce"),
#     "tot_energy": pd.to_numeric(df["Total kWh"], errors="coerce"),
#     "refuel_start": None,
#     "refuel_end": None,
#     "max_power": None
# })

# # Replace NaN with None for DB compatibility
# df_db = df_db.where(pd.notna(df_db), None)

# # Compute avg_power safely
# df_db["avg_power"] = df_db.apply(
#     lambda r: round((r.tot_energy * 60 / r.tot_ref_dura), 2)
#               if r.tot_energy is not None and r.tot_ref_dura and r.tot_ref_dura > 0 else None,
#     axis=1
# )

# # --- Insert into DB ---
# insert_sql = """
# INSERT INTO public.refuel_inf (
#     charger_id, veh_id, connect_time, disconnect_time,
#     refuel_start, refuel_end,
#     avg_power, max_power, tot_energy, tot_ref_dura
# )
# VALUES %s
# ON CONFLICT DO NOTHING;
# """

# cols = ["charger_id", "veh_id", "connect_time", "disconnect_time",
#         "refuel_start", "refuel_end",
#         "avg_power", "max_power", "tot_energy", "tot_ref_dura"]

# tuples = [tuple(x) for x in df_db[cols].to_numpy()]

# with engine.raw_connection() as conn:
#     with conn.cursor() as cur:
#         extras.execute_values(cur, insert_sql, tuples, template=None, page_size=1000)
#     conn.commit()

# print(f"Inserted {len(tuples)} rows into refuel_inf")
