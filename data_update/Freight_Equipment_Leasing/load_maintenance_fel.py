import os, sys
import pandas as pd
import numpy as np
import psycopg2.extras as extras
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.utils import to_boolean
from data_update.common_data_update import engine   # SQLAlchemy engine


# ==================== Config ====================
FOLDER_PATH = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Freight Equipment Leasing\maintenance data"
FILE_PATH = "\Freight Equipment Leasing Maintenance Events.xlsx"
XLSX_PATH = FOLDER_PATH + FILE_PATH

# -------------- Load all sheets --------------
df = pd.concat(pd.read_excel(XLSX_PATH, sheet_name=None), ignore_index=True)
# print(df.dtypes)



# Basic conversions
df["enter_shop"] = pd.to_datetime(df["The timestamp when vehicle entered the shop (yyyy-mm-dd hh24:mm)"], errors="coerce")
df["exit_shop"] = pd.to_datetime(df["The timestamp when vehicle exited the shop (yyyy-mm-dd hh24:mm)"], errors="coerce")
df["enter_odo"] = pd.to_numeric(df["Odometer reading upon entering shop (miles)"], errors="coerce").astype("Int64")
df["exit_odo"] = pd.to_numeric(df["Odometer reading upon exiting shop (miles)"], errors="coerce").astype("Int64")
df["parts_cost"] = pd.to_numeric(df["Parts cost ($)"], errors="coerce").astype("Float64")
df["labor_cost"] = pd.to_numeric(df["Labor cost ($)"], errors="coerce").astype("Float64")
df["add_cost"] = pd.to_numeric(df["Additional costs, if any ($) (please describe)"], errors="coerce").astype("Float64")
df["warranty"] = to_boolean(df["Warranty covered (yes or no)."])

df = df.rename(columns={
    "Maintenance category (identify all that apply) -  routine preventive maintenance, diagnostic, repair \n": "maint_categ",
    "Maintenance work performed in-house or outsourced?": "maint_loc",
    "If diagnostic or repair work, description of the condition or problem": "problem",
    "Description of the work performed": "work_perf",
})
# print(df)

# Split category
split_cols = df["maint_categ"].astype(str).str.split(":", n=1, expand=True)
df["maint_categ"] = split_cols[0].str.strip()
df["problem"] = split_cols[1].str.strip().fillna("") + ":" + df["problem"].fillna("")
# print(df)
# ==================== Map Vehicle ID ====================
with engine.begin() as conn:
    veh_map = pd.read_sql("SELECT id, fleet_vehicle_id FROM vehicle", conn)
veh_map = dict(zip(veh_map["fleet_vehicle_id"], veh_map["id"]))
df["vehicle_id"] = df["Vehicle ID (unique identifier)"].astype(str).map(veh_map).astype("Int64")

df["date"] = df["enter_shop"].dt.date
df["maint_ob"] = np.where(df["vehicle_id"].notna(), 1, np.nan)

df = df[[
    "date", "maint_ob", "vehicle_id", "maint_categ", "maint_loc",
    "enter_shop", "exit_shop", "enter_odo", "exit_odo",
    "parts_cost", "labor_cost", "add_cost",
    "warranty", "problem", "work_perf",
    ]].copy()
df = df.astype(object).where(pd.notna(df), None)
total_rows = len(df)

# Drop missing vehicle
mask_bad = df["vehicle_id"].isna()
n_drop_veh = int(mask_bad.sum())
df = df.loc[~mask_bad]
# print(df)


rows = [tuple(x) for x in df.to_numpy()]

# ==================== Insert ====================
sql = """
INSERT INTO public.maintenance (
    date, maint_ob, vehicle_id, maint_categ, maint_loc,
    enter_shop, exit_shop, enter_odo, exit_odo,
    parts_cost, labor_cost, add_cost,
    warranty, problem, work_perf
)
VALUES %s
ON CONFLICT DO NOTHING
RETURNING id
"""

inserted = 0
conn = engine.raw_connection()
try:
    with conn.cursor() as cur:
        ret = extras.execute_values(cur, sql, rows, page_size=1000, fetch=True)
        inserted = len(ret) if ret is not None else 0
    conn.commit()
finally:
    conn.close()

# ==================== Report ====================
attempted = len(df)
skipped_existing = attempted - inserted  # if you later add ON CONFLICT DO NOTHING
print("=== Maintenance Upload Summary ===")
print(f"Rows read from file:          {total_rows}")
print(f"Dropped: no vehicle match   {n_drop_veh}")
print(f"Rows attempted to insert:     {attempted}")
print(f"Inserted (new):               {inserted}")
print(f"Skipped (already existed):    {skipped_existing}")
print(f"[INFO] Inserted {inserted} Wilsbach maintenance rows.")