import pandas as pd
import psycopg2.extras as extras
import sys, os
from datetime import time as dt_time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine

# --- Config ---
FOLDER_PATH = r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Watsontown Trucking"
FILE_PATH = r"\2025 - Qtr 4\Charging & Telematics\WATW DEP EV Grant - Wattson - Q4 2025.xlsx"
CSV_PATH = FOLDER_PATH + FILE_PATH

# ---------- LOAD FILE ----------
_ext = os.path.splitext(CSV_PATH)[1].lower()
if _ext in (".xlsx", ".xls"):
    df = pd.read_excel(CSV_PATH)
else:
    # Fallback encodings for vendor CSV exports.
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(CSV_PATH, encoding="cp1252")
# print(df)

# Construct charger_id
df["charger_id_str"] = df["Serial Number"].astype(str).str.strip() + "-" + df["Connector Number"].astype(str).str.strip()

# Construct vehicle key from report (Tractor ID or Tractor Number only)
def _normalize_vehicle_key(v):
    if pd.isna(v):
        return None
    s = str(v).strip().upper().replace(" ", "")
    s = s.replace("?", "")
    return s if s else None

tractor_id = df.get("Tractor ID", pd.Series([None] * len(df))).apply(_normalize_vehicle_key)
tractor_num = df.get("Tractor Number", pd.Series([None] * len(df))).apply(_normalize_vehicle_key)
df["veh_key"] = tractor_id.where(tractor_id.notna(), tractor_num)

# Parse times
df["refuel_start"] = pd.to_datetime(df["Session Start Time"], errors="coerce", dayfirst=False)
df["refuel_end"]   = pd.to_datetime(df["Session Stop Time"],  errors="coerce", dayfirst=False)


# Numeric conversions
df["tot_energy"]   = pd.to_numeric(df["Energy Delivered (kWh)"], errors="coerce")

def _duration_to_minutes(v):
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timedelta):
        return v.total_seconds() / 60.0
    if isinstance(v, dt_time):
        return v.hour * 60.0 + v.minute + (v.second / 60.0) + (v.microsecond / 60000000.0)
    if isinstance(v, str):
        td = pd.to_timedelta(v, errors="coerce")
        return None if pd.isna(td) else td.total_seconds() / 60.0
    if isinstance(v, (int, float)):
        # Excel may store duration as fraction of day.
        return float(v) * 24.0 * 60.0
    td = pd.to_timedelta(v, errors="coerce")
    return None if pd.isna(td) else td.total_seconds() / 60.0

df["tot_ref_dura"] = df["Duration"].apply(_duration_to_minutes)
df["start_soc"]    = pd.to_numeric(df["Battery State Of Charge At Session Start"].astype(str).str.replace("%", ""), errors="coerce") / 100
df["end_soc"]      = pd.to_numeric(df["Battery State Of Charge At Session Stop"].astype(str).str.replace("%", ""), errors="coerce") / 100

# print(df)

# Filter invalid rows
valid = (
    df["refuel_start"].notna()
    & df["refuel_end"].notna()
    & (df["refuel_end"] > df["refuel_start"])
    & (df["tot_ref_dura"].fillna(0) > 0)
    & (df["tot_energy"].fillna(0) > 0)
)
print(f"[INFO] Dropping {(~valid).sum()} invalid rows (missing/invalid times or duration)")
df = df[valid].copy()

# Compute average power (kWh * 60 / min)
df["avg_power"] = df.apply(
    lambda r: round(r.tot_energy * 60 / r.tot_ref_dura, 2)
    if pd.notna(r.tot_energy) and pd.notna(r.tot_ref_dura) and r.tot_ref_dura > 0
    else None,
    axis=1
)

# Map charger string -> integer ID
with engine.connect() as conn:
    charger_map = pd.read_sql("SELECT id, charger FROM charger", conn)
    veh_map_df = pd.read_sql("SELECT id, fleet_vehicle_id FROM vehicle", conn)
charger_map = dict(zip(charger_map["charger"], charger_map["id"]))
veh_map = dict(zip(veh_map_df["fleet_vehicle_id"].astype(str).str.upper(), veh_map_df["id"]))
df["charger_id"] = df["charger_id_str"].map(charger_map).astype("Int64")
df["veh_id"] = df["veh_key"].map(veh_map).astype("Int64")

before = len(df)
df = df[df["charger_id"].notna()].copy()
print(f"[INFO] Dropped {before - len(df)} rows (chargers not found in DB)")
print(f"[INFO] Vehicle not mapped rows (veh_id=NULL): {int(df['veh_id'].isna().sum())}")

# ---------- UPLOAD ----------
df_db = df[[
    "charger_id", "veh_id", "refuel_start", "refuel_end", "avg_power",
    "tot_energy", "start_soc", "end_soc", "tot_ref_dura"
]].copy()

# Replace NaT / NaN → None
df_db = df_db.replace({pd.NaT: None})
df_db = df_db.where(pd.notna(df_db), None)

# print(df_db)

insert_sql = """
INSERT INTO public.refuel_inf (
    charger_id, veh_id, refuel_start, refuel_end, avg_power,
    tot_energy, start_soc, end_soc, tot_ref_dura
) VALUES %s
ON CONFLICT ON CONSTRAINT uq_refuel_session
DO UPDATE SET
  veh_id          = EXCLUDED.veh_id,
  refuel_start    = EXCLUDED.refuel_start,
  refuel_end      = EXCLUDED.refuel_end,
  avg_power       = EXCLUDED.avg_power,
  tot_energy      = EXCLUDED.tot_energy,
  start_soc       = EXCLUDED.start_soc,
  end_soc         = EXCLUDED.end_soc,
  tot_ref_dura    = EXCLUDED.tot_ref_dura;
"""

rows = [tuple(x) for x in df_db.to_numpy()]
conn = engine.raw_connection()
try:
    with conn.cursor() as cur:
        extras.execute_values(cur, insert_sql, rows, template=None, page_size=1000)
    conn.commit()
finally:
    conn.close()

print(f"[INFO] Upserted {len(rows)} Wattson charging sessions into refuel_inf.")
