import os, sys
import pandas as pd
import numpy as np
import psycopg2.extras as extras
from pytz.exceptions import AmbiguousTimeError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine   # SQLAlchemy engine

# ==================== Config ====================
FOLDER_PATH = r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Wilsbach Distributors\Telematics"
FILE_PATH = r"\Wilsbach EV Data Collection - Telematics Data - 12-2025.xlsx"
XLSX_PATH = FOLDER_PATH + FILE_PATH

DOUBLE_EPSILON = 0.05  # 5% tolerance for doubled-value artifact checks


def normalize_soc(x):
    if pd.isna(x):
        return None
    try:
        v = float(x)
    except Exception:
        return None
    v = v / 100.0
    return round(v, 4)


def _approx(a, b, eps=DOUBLE_EPSILON):
    if pd.isna(a) or pd.isna(b):
        return False
    scale = max(abs(float(b)), 1e-9)
    return abs(float(a) - float(b)) <= eps * scale


# ==================== Load ====================
df = pd.read_excel(XLSX_PATH)

# ---------- Parse & normalize timestamp (America/New_York -> UTC) ----------
dt_local = pd.to_datetime(df["Data Timestamp"], errors="coerce")

# Localize to America/New_York (handles DST), then convert to UTC
try:
    dt_local = dt_local.dt.tz_localize(
        "America/New_York",
        ambiguous="infer",          # infer DST fall-back where possible
        nonexistent="shift_forward" # shift through spring-forward gap
    )
except AmbiguousTimeError:
    # Fallback: ambiguous fall-back timestamps become NaT and are dropped later.
    dt_local = dt_local.dt.tz_localize(
        "America/New_York",
        ambiguous="NaT",
        nonexistent="shift_forward"
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

df = df[["veh_id", "timestamp", "elevation", "speed", "mileage", "soc", "key_on_time", "latitude", "longitude"]]


def correct_or_drop_double_artifacts(df):
    """
    Correction-first rule:
    - If mileage shows doubled artifact (curr ~= 2*prev), correct by dividing by 2.
    - If SOC shows doubled artifact (curr_soc ~= 2*prev_soc), correct by dividing by 2.
    - If corrected values are still invalid, drop the row.

    Note: monthly odometer discontinuities and reported elevation are kept as-is.
    """
    df = df.sort_values(["veh_id", "timestamp"]).reset_index(drop=True).copy()
    df["mileage"] = pd.to_numeric(df["mileage"], errors="coerce")
    df["soc"] = pd.to_numeric(df["soc"], errors="coerce")
    veh = df["veh_id"].to_numpy()
    mileage = df["mileage"].to_numpy(float)
    soc = df["soc"].to_numpy(float)

    keep = np.ones(len(df), dtype=bool)
    last_mileage = {}  # veh_id -> previous kept mileage
    last_soc = {}      # veh_id -> previous kept soc
    mileage_corrected = 0
    soc_corrected = 0

    for i in range(len(df)):
        v = veh[i]
        cur_mileage = mileage[i]
        cur_soc = soc[i]

        if np.isnan(cur_mileage):
            keep[i] = False
            continue

        prev_mileage = last_mileage.get(v, np.nan)
        prev_soc = last_soc.get(v, np.nan)

        if not np.isnan(prev_mileage):
            mileage_double = _approx(cur_mileage - prev_mileage, prev_mileage)
            if mileage_double:
                cur_mileage = cur_mileage / 2.0
                mileage_corrected += 1

        if (not np.isnan(prev_soc)) and (not np.isnan(cur_soc)):
            soc_double = _approx(cur_soc, 2.0 * prev_soc)
            if soc_double:
                cur_soc = cur_soc / 2.0
                soc_corrected += 1

        # If correction still yields invalid values, drop.
        if cur_mileage < 0:
            keep[i] = False
            continue
        if (not np.isnan(prev_mileage)) and (cur_mileage < prev_mileage * (1.0 - DOUBLE_EPSILON)):
            keep[i] = False
            continue
        if (not np.isnan(cur_soc)) and (cur_soc < 0 or cur_soc > 1):
                keep[i] = False
                continue

        df.at[i, "mileage"] = cur_mileage
        df.at[i, "soc"] = cur_soc
        last_mileage[v] = cur_mileage
        last_soc[v] = cur_soc

    dropped = int((~keep).sum())
    print(f"[Double-artifact correction] Mileage corrected: {mileage_corrected}, SOC corrected: {soc_corrected}, Dropped: {dropped}")
    return df.loc[keep].reset_index(drop=True), dropped, mileage_corrected, soc_corrected


# Apply correction-first artifact handling before basic validity filters
df, dropped, mileage_corrected, soc_corrected = correct_or_drop_double_artifacts(df)

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
df = df.sort_values(["veh_id", "timestamp"]).drop_duplicates(["veh_id", "timestamp"], keep="last")
drop_reasons["duplicates"] = before - len(df)

# ==================== Build final records ====================
def _py(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, np.generic):
        return v.item()
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

# ==================== Report ====================
attempted = len(df)
skipped_existing = attempted - inserted
total_dropped = sum(drop_reasons.values()) + dropped

print("=== Telemetry Upload Summary ===")
print(f"Rows read from file:          {total_rows}")
for k, v in drop_reasons.items():
    print(f"Dropped ({k.replace('_', ' ')}): {v}")
print(f"Corrected mileage doubles:    {mileage_corrected}")
print(f"Corrected SOC doubles:        {soc_corrected}")
print(f"Total dropped/removed:        {total_dropped}")
print(f"Rows attempted to insert:     {attempted}")
print(f"Inserted (new):               {inserted}")
print(f"Skipped (already existed):    {skipped_existing}")
