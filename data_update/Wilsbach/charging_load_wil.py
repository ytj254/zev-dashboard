import pandas as pd
import psycopg2.extras as extras
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine

# 1) Load Excel
FOLDER_PATH = r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Wilsbach Distributors\Charging event"
FILE_PATH = r"\Wilsbach EV Data Collection - Charging Event Data - 12-2025.xlsx"
XLSX_PATH = FOLDER_PATH + FILE_PATH
df = pd.read_excel(XLSX_PATH)

# 2) Basic normalization
df["charger_id_str"] = df["Charger ID"].astype(str).str.split(":").str[0] + "-" + df["Port"].astype(str)
df["veh_id_str"] = df["Vehicle ID"].astype(str)

df = df.rename(columns={
    "Connect Time": "connect_time",
    "Disconnect Time": "disconnect_time",
    "Charge Start Time": "refuel_start",
    "Charge End Time": "refuel_end",
    "Average Power": "avg_power",
    "Peak Power": "max_power",
    "Energy Dispensed": "tot_energy",
    "Vehicle SoC at start of Charging": "start_soc",
    "Vehicle SoC at end of Charging": "end_soc",
})

# print(df)
# 3) Parse datetimes BEFORE any .dt usage
for col in ["connect_time", "disconnect_time", "refuel_start", "refuel_end"]:
    df[col] = pd.to_datetime(df[col], errors="coerce")

# 4) Duration checks and validity filter
dur_min = (df["refuel_end"] - df["refuel_start"]).dt.total_seconds().div(60)
df["tot_ref_dura"] = dur_min.round(2)

valid = (
    df["connect_time"].notna()
    & df["disconnect_time"].notna()
    & (df["disconnect_time"] > df["connect_time"])
    & (df["tot_ref_dura"].fillna(0) > 0)
    & (df["tot_energy"].fillna(0) > 0)
)

dropped = (~valid).sum()
print(f"[INFO] Dropping {dropped} rows (zero/invalid duration or energy)")
df = df[valid].copy()

# 5) Normalize SoC (to fraction 0–1)
for col in ["start_soc", "end_soc"]:
    df[col] = (
        df[col].astype(str).str.replace("%", "", regex=False).str.strip()
        .replace({"": None})
    )
    df[col] = pd.to_numeric(df[col], errors="coerce") / 100.0

# 6) Compute avg_power safely (kWh * 60 / minutes)
df["tot_energy"] = pd.to_numeric(df["tot_energy"], errors="coerce")
df["avg_power"] = df.apply(
    lambda r: round(r.tot_energy * 60.0 / r.tot_ref_dura, 2)
              if pd.notna(r.tot_energy) and pd.notna(r.tot_ref_dura) and r.tot_ref_dura > 0 else None,
    axis=1
)

# 7) Map charger/vehicle strings -> integer IDs from DB
with engine.connect() as conn:
    ch = pd.read_sql("SELECT id, charger FROM charger", conn)
    vh = pd.read_sql("SELECT id, fleet_vehicle_id FROM vehicle", conn)

charger_map = dict(zip(ch["charger"].astype(str), ch["id"]))
veh_map     = dict(zip(vh["fleet_vehicle_id"].astype(str), vh["id"]))

df["charger_id"] = df["charger_id_str"].map(charger_map).astype("Int64")
df["veh_id"]     = df["veh_id_str"].map(veh_map).astype("Int64")

# keep only rows with known chargers (FK will fail otherwise)
before = len(df); df = df[df["charger_id"].notna()].copy()
print(f"[INFO] Dropped {before - len(df)} rows with chargers not in DB")
print(f"[INFO] Vehicles not mapped to DB (will insert NULL): {df['veh_id'].isna().sum()}")

# 8) Build DB-ready frame (Python datetimes for psycopg2)
df_db = df[[
    "charger_id","veh_id","connect_time","disconnect_time",
    "refuel_start","refuel_end","avg_power","max_power",
    "tot_energy","start_soc","end_soc","tot_ref_dura"
]].copy()

for col in ["connect_time","disconnect_time","refuel_start","refuel_end"]:
    df_db[col] = df_db[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)

df_db = df_db.where(pd.notna(df_db), None)
# print(df_db)
# 9) UPSERT on (charger_id, connect_time); update only if something changed
insert_sql = """
INSERT INTO public.refuel_inf (
    charger_id, veh_id, connect_time, disconnect_time,
    refuel_start, refuel_end, avg_power, max_power,
    tot_energy, start_soc, end_soc, tot_ref_dura
) VALUES %s
ON CONFLICT ON CONSTRAINT uq_refuel_session
DO UPDATE SET
  disconnect_time = EXCLUDED.disconnect_time,
  refuel_start    = EXCLUDED.refuel_start,
  refuel_end      = EXCLUDED.refuel_end,
  avg_power       = EXCLUDED.avg_power,
  max_power       = EXCLUDED.max_power,
  tot_energy      = EXCLUDED.tot_energy,
  start_soc       = EXCLUDED.start_soc,
  end_soc         = EXCLUDED.end_soc,
  veh_id          = EXCLUDED.veh_id,
  tot_ref_dura    = EXCLUDED.tot_ref_dura
WHERE
  refuel_inf.disconnect_time IS DISTINCT FROM EXCLUDED.disconnect_time OR
  refuel_inf.refuel_start    IS DISTINCT FROM EXCLUDED.refuel_start    OR
  refuel_inf.refuel_end      IS DISTINCT FROM EXCLUDED.refuel_end      OR
  refuel_inf.avg_power       IS DISTINCT FROM EXCLUDED.avg_power       OR
  refuel_inf.max_power       IS DISTINCT FROM EXCLUDED.max_power       OR
  refuel_inf.tot_energy      IS DISTINCT FROM EXCLUDED.tot_energy      OR
  refuel_inf.start_soc       IS DISTINCT FROM EXCLUDED.start_soc       OR
  refuel_inf.end_soc         IS DISTINCT FROM EXCLUDED.end_soc         OR
  refuel_inf.veh_id          IS DISTINCT FROM EXCLUDED.veh_id          OR
  refuel_inf.tot_ref_dura    IS DISTINCT FROM EXCLUDED.tot_ref_dura;
"""

# Ensure pandas NA -> None for psycopg2
df_db = df_db.map(lambda x: None if pd.isna(x) else x)

rows = [tuple(x) for x in df_db.to_numpy()]
conn = engine.raw_connection()
try:
    with conn.cursor() as cur:
        extras.execute_values(cur, insert_sql, rows, template=None, page_size=1000)
    conn.commit()
finally:
    conn.close()

print(f"[INFO] Upserted {len(rows)} Wilsbach charging events.")
