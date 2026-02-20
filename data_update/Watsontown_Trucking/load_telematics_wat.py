import pandas as pd
import numpy as np
import psycopg2.extras as extras
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine

# --- Config ---
FOLDER_PATH = r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Watsontown Trucking"
FILE_PATH = r"\2025 - Qtr 4\Charging & Telematics\EVJ2 Q4 2025 fuel path.csv"
CSV_PATH = FOLDER_PATH + FILE_PATH
GPS_MAX_CONSEC_JUMP_MILES = 5.0
# print(CSV_PATH)

# ---------- LOAD CSV ----------
df = pd.read_csv(CSV_PATH)
# print(df)

# ---------- Parse & normalize timestamp (America/New_York -> UTC) ----------
time_col = next((c for c in df.columns if c.lower().startswith("time(")), None)
if not time_col:
    raise RuntimeError("No time column found. Expected something like Time(EST) or Time(EDT).")

# Ensure a space between date and time; coerce errors to NaT
dt_local = pd.to_datetime(
    df["Date"].astype(str).str.strip() + " " + df[time_col].astype(str).str.strip(),
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
df["mileage_raw"] = pd.to_numeric(df["Distance Traveled(Miles)"], errors="coerce")
df["latitude"] = pd.to_numeric(df["Lat"], errors="coerce")
df["longitude"] = pd.to_numeric(df["Lon"], errors="coerce")
# print(df)

# ---------- Map Asset No. -> vehicle.id ----------
with engine.begin() as conn:
    veh_map = pd.read_sql("SELECT id, fleet_vehicle_id FROM vehicle", conn)
veh_map = dict(zip(veh_map["fleet_vehicle_id"], veh_map["id"]))
df["veh_id"] = df["Asset No."].map(veh_map).astype("Int64")


df = df[["veh_id", "timestamp", "speed", "mileage_raw", "latitude", "longitude"]]
# print(df)

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

# ---------- GPS outlier filter (null out bad coordinates) ----------
def _haversine_miles(lat1, lon1, lat2, lon2):
    r = 3958.7613  # Earth radius in miles
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (
        np.sin(dlat / 2.0) ** 2
        + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2.0) ** 2
    )
    return 2 * r * np.arcsin(np.sqrt(a))


gps_outlier_mask = pd.Series(False, index=df.index)
for _, g in df.groupby("veh_id", sort=False):
    g2 = g.sort_values("timestamp").reset_index()  # keep original df index in column 'index'
    n = len(g2)
    if n < 2:
        continue

    lat = g2["latitude"].to_numpy()
    lon = g2["longitude"].to_numpy()
    ts = g2["timestamp"].to_numpy()
    has_coord = (~pd.isna(lat)) & (~pd.isna(lon))
    flags = np.zeros(n, dtype=bool)

    def transition_stats(i_from: int, i_to: int):
        if i_from < 0 or i_to <= i_from:
            return None
        if not has_coord[i_from] or not has_coord[i_to]:
            return None
        dt_sec = (pd.Timestamp(ts[i_to]) - pd.Timestamp(ts[i_from])).total_seconds()
        if dt_sec <= 0:
            return None
        jump = _haversine_miles(lat[i_from], lon[i_from], lat[i_to], lon[i_to])
        implied = jump / (dt_sec / 3600.0)
        return dt_sec, jump, implied

    def is_jump_transition(i_from: int, i_to: int):
        stats = transition_stats(i_from, i_to)
        if stats is None:
            return False
        _dt_sec, jump, _implied = stats
        # User rule: compare consecutive points spatially, independent of time gap.
        return jump > GPS_MAX_CONSEC_JUMP_MILES

    prev_normal = None
    for i in range(n):
        if not has_coord[i]:
            continue
        prev_normal = i
        break

    if prev_normal is None:
        continue

    i = prev_normal + 1
    while i < n:
        if not has_coord[i]:
            i += 1
            continue

        # First detect if this point is a jumper based on the immediate transition.
        if not is_jump_transition(i - 1, i):
            prev_normal = i
            i += 1
            continue

        # Start jumper cluster at i.
        flags[i] = True
        jumper_ref = i
        anchor = prev_normal
        k = i + 1
        returned_to_normal = False

        while k < n:
            if not has_coord[k]:
                break

            adj_stats = transition_stats(k - 1, k)
            if adj_stats is None:
                break

            # Compare current point to previous normal anchor vs jumper cluster reference.
            d_anchor = _haversine_miles(lat[anchor], lon[anchor], lat[k], lon[k]) if anchor is not None else np.inf
            d_jumper = _haversine_miles(lat[jumper_ref], lon[jumper_ref], lat[k], lon[k])

            if d_anchor <= d_jumper:
                # Returned near normal track: retain this point and exit cluster.
                prev_normal = k
                returned_to_normal = True
                break

            # Still near jumper cluster: mark as outlier and continue cluster.
            flags[k] = True
            jumper_ref = k
            k += 1

        # If row k is accepted as the return-to-normal point, skip re-evaluating it
        # as a potential new jumper in the outer loop.
        i = k + 1 if returned_to_normal else k

    gps_outlier_mask.loc[g2["index"].to_numpy()] = flags

n_gps_outlier = int(gps_outlier_mask.sum())
if n_gps_outlier:
    df.loc[gps_outlier_mask, ["latitude", "longitude"]] = np.nan

# ---------- Rebuild mileage with DB anchor ----------
def _build_monotonic(raw: pd.Series) -> pd.Series:
    """Convert reset-prone cumulative counter to monotonic cumulative mileage."""
    out = pd.Series(index=raw.index, dtype="float64")
    if raw.notna().sum() == 0:
        return out

    first_valid_label = raw.first_valid_index()
    first_pos = raw.index.get_loc(first_valid_label)

    raw2 = raw.copy()
    raw2.iloc[:first_pos + 1] = raw.iloc[first_pos]
    raw2 = raw2.ffill()

    delta = raw2.diff().fillna(0.0)
    delta_pos = delta.clip(lower=0.0)
    out = raw2.iloc[0] + delta_pos.cumsum()
    return out


df["mileage"] = np.nan
with engine.begin() as conn:
    for veh_id, g in df.groupby("veh_id", sort=False):
        g = g.sort_values("timestamp")
        rebuilt = _build_monotonic(g["mileage_raw"])

        anchor = pd.read_sql(
            """
            SELECT mileage
            FROM veh_tel
            WHERE veh_id = %s
              AND "timestamp" < %s
              AND mileage IS NOT NULL
            ORDER BY "timestamp" DESC
            LIMIT 1
            """,
            conn,
            params=(int(veh_id), g["timestamp"].iloc[0].to_pydatetime()),
        )
        if not anchor.empty and pd.notna(anchor.iloc[0]["mileage"]):
            shift = float(anchor.iloc[0]["mileage"]) - float(rebuilt.iloc[0])
            rebuilt = rebuilt + shift

        df.loc[g.index, "mileage"] = rebuilt.values

# ---------- Pre-upsert overlap diagnostics ----------
incoming_cmp = df[["veh_id", "timestamp", "speed", "mileage", "latitude", "longitude"]].copy()
overlap_existing = new_rows = changed_overlap = unchanged_overlap = 0

if not incoming_cmp.empty:
    veh_ids = [int(x) for x in sorted(incoming_cmp["veh_id"].dropna().astype(int).unique())]
    min_ts = incoming_cmp["timestamp"].min().to_pydatetime()
    max_ts = incoming_cmp["timestamp"].max().to_pydatetime()

    with engine.connect() as conn:
        existing_cmp = pd.read_sql(
            """
            SELECT veh_id, "timestamp", speed, mileage, latitude, longitude
            FROM veh_tel
            WHERE veh_id = ANY(%s)
              AND "timestamp" BETWEEN %s AND %s
            """,
            conn,
            params=(veh_ids, min_ts, max_ts),
        )

    merged = incoming_cmp.merge(
        existing_cmp,
        on=["veh_id", "timestamp"],
        how="left",
        suffixes=("_new", "_old"),
        indicator=True,
    )
    overlap_mask = merged["_merge"].eq("both")
    new_mask = merged["_merge"].eq("left_only")

    def _eq(a, b, nd):
        return (a.round(nd) == b.round(nd)) | (a.isna() & b.isna())

    same_mask = (
        _eq(merged["speed_new"], merged["speed_old"], 6)
        & _eq(merged["mileage_new"], merged["mileage_old"], 6)
        & _eq(merged["latitude_new"], merged["latitude_old"], 8)
        & _eq(merged["longitude_new"], merged["longitude_old"], 8)
    )

    overlap_existing = int(overlap_mask.sum())
    new_rows = int(new_mask.sum())
    unchanged_overlap = int((overlap_mask & same_mask).sum())
    changed_overlap = int((overlap_mask & ~same_mask).sum())

# ---------- Insert (ON CONFLICT UPDATE) ----------
def _py(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    if isinstance(v, np.generic): return v.item()  # np.int64/float64 -> py
    return v

records = []
for vid, ts, sp, mi, la, lo in df[["veh_id","timestamp","speed","mileage","latitude","longitude"]].itertuples(index=False, name=None):
    records.append((
        int(vid),                               # veh_id
        ts.to_pydatetime(),                     # tz-aware datetime
        _py(sp), _py(mi), _py(la), _py(lo),
        _py(lo), _py(la), _py(lo), _py(la)      # for conditional ST_MakePoint(lon, lat)
    ))
    
upserted = 0
if records:
    sql = """
    INSERT INTO veh_tel (veh_id, "timestamp", speed, mileage, latitude, longitude, location)
    VALUES %s
    ON CONFLICT (veh_id, "timestamp") DO UPDATE SET
      speed = EXCLUDED.speed,
      mileage = EXCLUDED.mileage,
      latitude = EXCLUDED.latitude,
      longitude = EXCLUDED.longitude,
      location = EXCLUDED.location
    WHERE
      veh_tel.speed IS DISTINCT FROM EXCLUDED.speed OR
      veh_tel.mileage IS DISTINCT FROM EXCLUDED.mileage OR
      veh_tel.latitude IS DISTINCT FROM EXCLUDED.latitude OR
      veh_tel.longitude IS DISTINCT FROM EXCLUDED.longitude OR
      veh_tel.location IS DISTINCT FROM EXCLUDED.location
    RETURNING id;
    """
    template = "(%s,%s,%s,%s,%s,%s, CASE WHEN %s IS NOT NULL AND %s IS NOT NULL THEN ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography ELSE NULL END)"

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            ret = extras.execute_values(cur, sql, records, template=template, page_size=5000, fetch=True)
            upserted = len(ret) if ret is not None else 0
        conn.commit()
    finally:
        conn.close()

# ---------- Report ----------
attempted = len(df)
unchanged_existing = attempted - upserted
total_dropped = n_drop_time + n_drop_veh + n_dedup

print("=== Telemetry Upload Summary ===")
print(f"CSV rows read:              {total_rows}")
print(f"Dropped: invalid timestamp  {n_drop_time}")
print(f"Dropped: no vehicle match   {n_drop_veh}")
print(f"Removed duplicates in CSV:  {n_dedup}")
print(f"GPS outlier coords nulled:  {n_gps_outlier}")
print(f"Rows attempted to insert:   {attempted}")
print(f"Overlap with existing:      {overlap_existing}")
print(f"New rows (no overlap):      {new_rows}")
print(f"Changed overlap rows:       {changed_overlap}")
print(f"Unchanged overlap rows:     {unchanged_overlap}")
print(f"Rows inserted/updated:      {upserted}")
print(f"Rows unchanged in DB:       {unchanged_existing}")
print(f"Total dropped/removed:      {total_dropped}")
