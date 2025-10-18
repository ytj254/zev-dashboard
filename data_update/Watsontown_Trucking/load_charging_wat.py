import pandas as pd
import psycopg2.extras as extras
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine

# --- Config ---
CSV_PATH = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Watsontown Trucking\Charging & Telematics_Qtr 3 2025\WATW ABB Charger Session - Wattson.csv"

# ---------- LOAD CSV ----------
df = pd.read_csv(CSV_PATH)
# print(df)

# Construct charger_id
df["charger_id_str"] = df["Serial Number"].astype(str).str.strip() + "-" + df["Connector Number"].astype(str).str.strip()

# Parse times
df["refuel_start"] = pd.to_datetime(df["Session Start Time"], errors="coerce", dayfirst=False)
df["refuel_end"]   = pd.to_datetime(df["Session Stop Time"],  errors="coerce", dayfirst=False)


# Numeric conversions
df["tot_energy"]   = pd.to_numeric(df["Energy Delivered (kWh)"], errors="coerce")
df["tot_ref_dura"] = (pd.to_timedelta(df["Duration"], errors="coerce").dt.total_seconds() / 60)
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
charger_map = dict(zip(charger_map["charger"], charger_map["id"]))
df["charger_id"] = df["charger_id_str"].map(charger_map).astype("Int64")

before = len(df)
df = df[df["charger_id"].notna()].copy()
print(f"[INFO] Dropped {before - len(df)} rows (chargers not found in DB)")

# ---------- UPLOAD ----------
df_db = df[[
    "charger_id", "refuel_start", "refuel_end", "avg_power",
    "tot_energy", "start_soc", "end_soc", "tot_ref_dura"
]].copy()

# Replace NaT / NaN → None
df_db = df_db.replace({pd.NaT: None})
df_db = df_db.where(pd.notna(df_db), None)

# print(df_db)

insert_sql = """
INSERT INTO public.refuel_inf (
    charger_id, refuel_start, refuel_end, avg_power,
    tot_energy, start_soc, end_soc, tot_ref_dura
) VALUES %s
ON CONFLICT ON CONSTRAINT uq_refuel_session
DO UPDATE SET
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