import pandas as pd
import psycopg2.extras as extras
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine

# --- Config ---
EXCEL_FILE = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Freight Equipment Leasing\Charging log\Charge Log - 2025-10-27 10_23_31.xlsx"

# --- Helpers ---
def normalize_charger(charger_str: str) -> str | None:
    """Turn 'C03, Sae J1772 Combo United States, 1' → 'C03P1'"""
    if pd.isna(charger_str):
        return None
    parts = [p.strip() for p in str(charger_str).split(",")]
    if len(parts) >= 2:
        return f"{parts[0]}P{parts[-1]}"
    return None

def fetch_vehicle_map():
    """Return {fleet_vehicle_id -> vehicle.id} for all vehicles in DB"""
    with engine.connect() as conn:
        df = pd.read_sql("SELECT id, fleet_vehicle_id FROM vehicle", conn)
    return dict(zip(df["fleet_vehicle_id"].dropna().astype(str), df["id"]))

def fetch_charger_map():
    """Return {charger string -> charger.id} for all chargers in DB"""
    with engine.connect() as conn:
        df = pd.read_sql("SELECT id, charger FROM charger", conn)
    return dict(zip(df["charger"].dropna().astype(str), df["id"]))

def parse_duration(val):
    """Convert 'hh:mm:ss' string → minutes with decimals"""
    if pd.isna(val):
        return None
    try:
        h, m, s = map(int, str(val).split(":"))
        return round(h * 60 + m + s / 60, 2)  # total minutes
    except Exception:
        return None

# --- Load Excel ---
df = pd.read_excel(EXCEL_FILE)
df.columns = df.columns.str.strip()  # strip spaces just in case
# print(f'Original db: {df}')

# Normalize charger IDs
df["charger_id"] = df["Charger"].apply(normalize_charger)
# print(f'Normalized db: {df}')

# Get DB maps
veh_map = fetch_vehicle_map()
charger_map = fetch_charger_map()

# Count before filter
before = len(df)

# Filter chargers to only those known in DB
df = df[df["charger_id"].isin(charger_map.keys())]
# print(f'Filtered db: {df}')

# Report dropped chargers
dropped_chargers = before - len(df)
print(f"[INFO] Dropped {dropped_chargers} rows with chargers not in DB")

# Map IDs to integers (unmapped → NaN → later None)
df["veh_id"] = df["Linked license plate"].astype(str).map(veh_map).astype("Int64")
df["charger_id"] = df["charger_id"].map(charger_map).astype("Int64")

# Report vehicles not mapped
unmapped_vehicles = df["veh_id"].isna().sum()
print(f"[INFO] Vehicles not mapped to DB (will insert as NULL): {unmapped_vehicles}")

# Build DB-ready DataFrame
df_db = pd.DataFrame({
    "charger_id": df["charger_id"],
    "veh_id": df["veh_id"],
    "connect_time": pd.to_datetime(df["Start Date"], errors="coerce"),
    "disconnect_time": pd.to_datetime(df["End Date"], errors="coerce"),
    "tot_ref_dura": df["Total Charging Time"].map(parse_duration),
    "tot_energy": pd.to_numeric(df["Total kWh"], errors="coerce"),
})

# Compute avg_power safely
df_db["avg_power"] = df_db.apply(
    lambda r: round((r.tot_energy * 60 / r.tot_ref_dura), 2)
              if r.tot_energy is not None and r.tot_ref_dura and r.tot_ref_dura > 0 else None,
    axis=1
)

# derive duration from timestamps to double-check vendor field
dur_min_from_ts = (df_db["disconnect_time"] - df_db["connect_time"]).dt.total_seconds() / 60
df_db["dur_check"] = dur_min_from_ts.round(2)

valid = (
    df_db["connect_time"].notna()
    & df_db["disconnect_time"].notna()
    & (df_db["disconnect_time"] > df_db["connect_time"])
    & (df_db["tot_ref_dura"].fillna(0) > 0)
    & (df_db["dur_check"].fillna(0) > 0)
    & (df_db["tot_energy"].fillna(0) > 0)
)

dropped = (~valid).sum()
print(f"[INFO] Dropping {dropped} rows (zero/invalid duration or energy)")

df_db = df_db[valid].drop(columns=["dur_check"])

# Replace NaT / NaN → None
df_db = df_db.replace({pd.NaT: None})
df_db = df_db.where(pd.notna(df_db), None)

print(df_db)

# --- Insert into DB ---
insert_sql = """
INSERT INTO public.refuel_inf (
    charger_id, veh_id, connect_time, disconnect_time,
    avg_power, tot_energy, tot_ref_dura
) VALUES %s
ON CONFLICT ON CONSTRAINT uq_refuel_session
DO UPDATE SET
  disconnect_time = EXCLUDED.disconnect_time,
  avg_power       = EXCLUDED.avg_power,
  tot_energy      = EXCLUDED.tot_energy,
  tot_ref_dura    = EXCLUDED.tot_ref_dura,
  veh_id          = EXCLUDED.veh_id,
  connect_time    = EXCLUDED.connect_time
WHERE
  refuel_inf.disconnect_time IS DISTINCT FROM EXCLUDED.disconnect_time OR
  refuel_inf.avg_power       IS DISTINCT FROM EXCLUDED.avg_power       OR
  refuel_inf.tot_energy      IS DISTINCT FROM EXCLUDED.tot_energy      OR
  refuel_inf.tot_ref_dura    IS DISTINCT FROM EXCLUDED.tot_ref_dura    OR
  refuel_inf.veh_id          IS DISTINCT FROM EXCLUDED.veh_id OR
  refuel_inf.connect_time    IS DISTINCT FROM EXCLUDED.connect_time;
"""

cols = ["charger_id", "veh_id", "connect_time", "disconnect_time",
        "avg_power", "tot_energy", "tot_ref_dura"]

tuples = [tuple(x) for x in df_db[cols].to_numpy()]

conn = engine.raw_connection()
try:
    with conn.cursor() as cur:
        extras.execute_values(cur, insert_sql, tuples, template=None, page_size=1000)
    conn.commit()
finally:
    conn.close()

print(f"Upserted {len(tuples)} rows into refuel_inf")
