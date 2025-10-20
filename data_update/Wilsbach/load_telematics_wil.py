import os, sys
import pandas as pd
import numpy as np
import psycopg2.extras as extras
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine   # SQLAlchemy engine

# ==================== Config ====================
FOLDER_PATH = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Wilsbach Distributors\Telematics"
FILE_PATH = "\EV Data Collection - NEW Telematics Data - 04-2025.xlsx"
XLSX_PATH = FOLDER_PATH + FILE_PATH

MAX_ODO_JUMP = 5               # miles between records
MAX_ELEV_JUMP = 50              # meters
EPSILON = 0.05   # 5% tolerance (so 1.9–2.1× counts as “double”)

def normalize_soc(x):
    if pd.isna(x):
        return None
    try:
        v = float(x)
    except Exception:
        return None
    v = v / 100.0
    return round(v, 4)

# ==================== Load ====================
df = pd.read_excel(XLSX_PATH)
# print(df.dtypes)

# ---------- Parse & normalize timestamp (EDT -> UTC) ----------
# Ensure a space between date and time; coerce errors to NaT
dt_local = pd.to_datetime(df["Data Timestamp"], errors="coerce")

# Localize to America/New_York (handles DST), then convert to UTC
dt_local = dt_local.dt.tz_localize(
    "America/New_York",
    ambiguous="infer",          # infer DST fall-back
    nonexistent="shift_forward" # shift through spring-forward gap
)
df["timestamp"] = dt_local.dt.tz_convert("UTC")

# Numeric conversions
df["speed"] = pd.to_numeric(df["Speed"], errors="coerce")
df["mileage"] = pd.to_numeric(df["Odometer"], errors="coerce")
df["latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
df["elevation"] = pd.to_numeric(df["Elevation"], errors="coerce")
df["soc"] = df["State Of Charge"].apply(normalize_soc)
df["key_on_time"] = pd.to_numeric(df["Total Travel Time (Hrs)"], errors="coerce")

# ==================== Map Vehicle ID ====================
with engine.begin() as conn:
    veh_map = pd.read_sql("SELECT id, fleet_vehicle_id FROM vehicle", conn)
veh_map = dict(zip(veh_map["fleet_vehicle_id"], veh_map["id"]))
df["veh_id"] = df["Vehicle ID"].astype(str).map(veh_map).astype("Int64")
# print(df)

df = df[["veh_id","timestamp","elevation","speed","mileage","soc","key_on_time","latitude","longitude"]]
# print(df)

def drop_mileage_doubles(df):
    """Drop rows where SOC is roughly double the previous kept row."""
    df = df.sort_values(["veh_id", "timestamp"]).reset_index(drop=True).copy()
    veh = df["veh_id"].to_numpy()
    mileage = pd.to_numeric(df["mileage"], errors="coerce").to_numpy(float)

    keep = np.ones(len(df), dtype=bool)
    last_mileage = {}  # veh_id -> previous kept SOC

    for i in range(len(df)):
        v, s = veh[i], mileage[i]
        if np.isnan(s):
            keep[i] = False
            continue

        prev = last_mileage.get(v, np.nan)
        if not np.isnan(prev):
            # if current ≈ 2× previous → drop
            if prev * (1-EPSILON) < s - prev < prev * (1+EPSILON):
                keep[i] = False
                continue

        # keep row and update baseline
        last_mileage[v] = s
        # print(last_soc)
    dropped = (~keep).sum()
    print(f"[Mileage double filter] Dropped {dropped} rows.")
    return df.loc[keep].reset_index(drop=True), dropped

# ---- use it before converting SOC to 0–1 ----
df, dropped = drop_mileage_doubles(df)

# ==================== Drop invalid core data ====================
total_rows = len(df)
drop_reasons = {}

# 1. Drop missing vehicle or timestamp
mask_bad = df["veh_id"].isna() | df["timestamp"].isna()
drop_reasons["missing_veh_or_ts"] = int(mask_bad.sum())
df = df.loc[~mask_bad]

# 2. Drop invalid GPS
mask_bad_gps = (~df["latitude"].between(-90, 90)) | (~df["longitude"].between(-180, 180))
drop_reasons["invalid_gps"] = int(mask_bad_gps.sum())
df = df.loc[~mask_bad_gps]

# 3. Drop bad speed
mask_bad_speed = df["speed"] < 0
drop_reasons["bad_speed"] = int(mask_bad_speed.sum())
df = df.loc[~mask_bad_speed]

# 4. Drop bad SOC
mask_bad_soc = (df["soc"] < 0) | (df["soc"] > 1)
drop_reasons["bad_soc"] = int(mask_bad_soc.sum())
df = df.loc[~mask_bad_soc]

# 5. Drop duplicates
before = len(df)
df = df.sort_values(["veh_id","timestamp"]).drop_duplicates(["veh_id","timestamp"], keep="last")
drop_reasons["duplicates"] = before - len(df)
# print(df)

# ==================== Build final records ====================
def _py(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    if isinstance(v, np.generic): return v.item()
    return v

records = []
for vid, ts, elev, sp, mil, soc, key, la, lo in df.itertuples(index=False, name=None):
    records.append((
        int(vid),
        ts.to_pydatetime(),
        _py(elev), _py(sp), _py(mil), _py(soc), _py(key), _py(la), _py(lo),
        _py(lo), _py(la)  # used only inside ST_MakePoint(lon, lat)
    ))

# ==================== Insert ====================
inserted = 0
if records:
    sql = """
    INSERT INTO veh_tel
    (veh_id, "timestamp", elevation, speed, mileage, soc, key_on_time, latitude, longitude, location)
    VALUES %s
    ON CONFLICT (veh_id, "timestamp") DO NOTHING
    RETURNING 1;
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)"

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            ret = extras.execute_values(cur, sql, records, template=template, page_size=5000, fetch=True)
            inserted = len(ret)
        conn.commit()
    finally:
        conn.close()

# # ==================== Report ====================
attempted = len(df)
skipped_existing = attempted - inserted
total_dropped = sum(drop_reasons.values()) + dropped

print("=== Telemetry Upload Summary ===")
print(f"Rows read from file:          {total_rows}")
for k,v in drop_reasons.items():
    print(f"Dropped ({k.replace('_',' ')}): {v}")
print(f"Total dropped/removed:        {total_dropped}")
print(f"Rows attempted to insert:     {attempted}")
print(f"Inserted (new):               {inserted}")
print(f"Skipped (already existed):    {skipped_existing}")
