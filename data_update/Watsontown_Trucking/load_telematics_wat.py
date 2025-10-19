import pandas as pd
import numpy as np
import psycopg2.extras as extras
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine

# --- Config ---
FOLDER_PATH = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Watsontown Trucking"
FILE_PATH = "\WATW EV Reports 020125-073125\WATW Zonar EV7 Fuel Path 020125-073125.csv"
CSV_PATH = FOLDER_PATH + FILE_PATH
# print(CSV_PATH)

# ---------- LOAD CSV ----------
df = pd.read_csv(CSV_PATH)
# print(df)

# ---------- Parse & normalize timestamp (EDT -> UTC) ----------
# Ensure a space between date and time; coerce errors to NaT
dt_local = pd.to_datetime(
    df["Date"].astype(str).str.strip() + " " + df["Time(EDT)"].astype(str).str.strip(),
    errors="coerce"
)

# Localize to America/New_York (handles DST), then convert to UTC
dt_local = dt_local.dt.tz_localize(
    "America/New_York",
    ambiguous="infer",          # infer DST fall-back
    nonexistent="shift_forward" # shift through spring-forward gap
)
df["timestamp"] = dt_local.dt.tz_convert("UTC")

# Numeric conversions
df["speed"] = pd.to_numeric(df["Speed(MPH)"], errors="coerce")
df["mileage"] = pd.to_numeric(df["Distance Traveled(Miles)"], errors="coerce")
df["latitude"] = pd.to_numeric(df["Lat"], errors="coerce")
df["longitude"] = pd.to_numeric(df["Lon"], errors="coerce")
# print(df)

# ---------- Map Asset No. -> vehicle.id ----------
with engine.begin() as conn:
    veh_map = pd.read_sql("SELECT id, fleet_vehicle_id FROM vehicle", conn)
veh_map = dict(zip(veh_map["fleet_vehicle_id"], veh_map["id"]))
df["veh_id"] = df["Asset No."].map(veh_map).astype("Int64")


df = df[["veh_id", "timestamp", "speed", "mileage", "latitude", "longitude"]]
print(df)

# ---------- Data quality filters & counts ----------
total_rows = len(df)

# Drop rows with missing or invalid timestamp / vehicle / coords
bad_time   = df["timestamp"].isna()
bad_veh    = df["veh_id"].isna()

drop_mask  = bad_time | bad_veh
n_drop_time, n_drop_veh = int(bad_time.sum()), int(bad_veh.sum())

df = df.loc[~drop_mask].copy()

# De-dupe within this CSV
before = len(df)
df = df.sort_values(["veh_id", "timestamp"]).drop_duplicates(["veh_id", "timestamp"], keep="last")
n_dedup = before - len(df)
# print(df.dtypes)

# ---------- Insert (ON CONFLICT DO NOTHING) ----------
def _py(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    if isinstance(v, np.generic): return v.item()  # np.int64/float64 -> py
    return v

inserted = 0
records = []
for vid, ts, sp, mi, la, lo in df[["veh_id","timestamp","speed","mileage","latitude","longitude"]].itertuples(index=False, name=None):
    records.append((
        int(vid),                               # veh_id
        ts.to_pydatetime(),                     # tz-aware datetime
        _py(sp), _py(mi), _py(la), _py(lo),
        _py(lo), _py(la)                        # for ST_MakePoint(lon, lat)
    ))
sql = """
INSERT INTO veh_tel (veh_id, "timestamp", speed, mileage, latitude, longitude, location)
VALUES %s
ON CONFLICT (veh_id, "timestamp") DO NOTHING
RETURNING id;
"""
template = "(%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)"

conn = engine.raw_connection()
try:
    with conn.cursor() as cur:
        ret = extras.execute_values(cur, sql, records, template=template, page_size=5000, fetch=True)
        inserted = len(ret)  # number of rows actually inserted
    conn.commit()
finally:
    conn.close()

# ---------- Report ----------
attempted = len(df)
skipped_existing = attempted - inserted
total_dropped = n_drop_time + n_drop_veh + n_dedup

print("=== Telemetry Upload Summary ===")
print(f"CSV rows read:              {total_rows}")
print(f"Dropped: invalid timestamp  {n_drop_time}")
print(f"Dropped: no vehicle match   {n_drop_veh}")
print(f"Removed duplicates in CSV:  {n_dedup}")
print(f"Rows attempted to insert:   {attempted}")
print(f"Inserted (new):             {inserted}")
print(f"Skipped (already existed):  {skipped_existing}")
print(f"Total dropped/removed:      {total_dropped}")