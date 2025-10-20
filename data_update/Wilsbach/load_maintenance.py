import os, sys
import pandas as pd
import numpy as np
import psycopg2.extras as extras
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.utils import to_boolean
from data_update.common_data_update import engine   # SQLAlchemy engine

# ==================== Config ====================
FOLDER_PATH = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Wilsbach Distributors\maintenance"
FILE_PATH = "\EV Data Collection - Vehicle Maintenance - Mar-Sep-2025.xlsx"
XLSX_PATH = FOLDER_PATH + FILE_PATH

# ==================== Load ====================
df = pd.read_excel(XLSX_PATH)
# print(df)

# Basic conversions
df["enter_shop"] = pd.to_datetime(df["Date to Shop"], errors="coerce")
df["exit_shop"] = pd.to_datetime(df["Date Returned"], errors="coerce")
df["enter_odo"] = pd.to_numeric(df["Start Odometer"], errors="coerce").astype("Int64")
df["exit_odo"] = pd.to_numeric(df["Returned Odometer"], errors="coerce").astype("Int64")
df["parts_cost"] = pd.to_numeric(df["Parts Costs"], errors="coerce").astype("Float64")
df["labor_cost"] = pd.to_numeric(df["Labor Costs"], errors="coerce").astype("Float64")
df["add_cost"] = pd.to_numeric(df["Added Costs"], errors="coerce").astype("Float64")
df["warranty"] = to_boolean(df["Warranty Coverage?"])

df = df.rename(columns={
    "Category": "maint_categ",
    "Location": "maint_loc",
    "Added Costs Desc": "add_cost_desc",
    "Desc of Problem": "problem",
    "Desc of Work Done": "work_perf",
})


# ==================== Map Vehicle ID ====================
with engine.begin() as conn:
    veh_map = pd.read_sql("SELECT id, fleet_vehicle_id FROM vehicle", conn)
veh_map = dict(zip(veh_map["fleet_vehicle_id"], veh_map["id"]))
df["vehicle_id"] = df["Vehicle ID"].astype(str).map(veh_map).astype("Int64")

df["date"] = df["enter_shop"].dt.date
df["maint_ob"] = np.where(df["vehicle_id"].notna(), 1, np.nan)

df = df[[
    "date", "maint_ob", "vehicle_id", "maint_categ", "maint_loc",
    "enter_shop", "exit_shop", "enter_odo", "exit_odo",
    "parts_cost", "labor_cost", "add_cost", "add_cost_desc",
    "warranty", "problem", "work_perf",
    ]].copy()
df = df.astype(object).where(pd.notna(df), None)
# print(df)
total_rows = len(df)
rows = [tuple(x) for x in df.to_numpy()]

# ==================== Insert ====================
sql = """
INSERT INTO public.maintenance (
    date, maint_ob, vehicle_id, maint_categ, maint_loc,
    enter_shop, exit_shop, enter_odo, exit_odo,
    parts_cost, labor_cost, add_cost, add_cost_desc,
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
attempted = total_rows
skipped_existing = attempted - inserted  # if you later add ON CONFLICT DO NOTHING
print("=== Maintenance Upload Summary ===")
print(f"Rows read from file:          {total_rows}")
print(f"Rows attempted to insert:     {attempted}")
print(f"Inserted (new):               {inserted}")
print(f"Skipped (already existed):    {skipped_existing}")
print(f"[INFO] Inserted {inserted} Wilsbach maintenance rows.")