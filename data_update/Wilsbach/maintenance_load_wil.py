import os, sys
import pandas as pd
import numpy as np
import psycopg2.extras as extras
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.utils import to_boolean
from data_update.common_data_update import engine   # SQLAlchemy engine

# ==================== Config ====================
FOLDER_PATH = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Wilsbach Distributors\maintenance"
FILE_PATH = "\Wilsbach EV Data Collection - Vehicle Maintenance - Mar-2025 - Current - 0126.xlsx"
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
df["veh_id"] = df["Vehicle ID"].astype(str).map(veh_map).astype("Int64")

df["date"] = df["enter_shop"].dt.date
df["maint_ob"] = np.where(df["veh_id"].notna(), 1, np.nan)

df = df[[
    "date", "maint_ob", "veh_id", "maint_categ", "maint_loc",
    "enter_shop", "exit_shop", "enter_odo", "exit_odo",
    "parts_cost", "labor_cost", "add_cost", "add_cost_desc",
    "warranty", "problem", "work_perf",
    ]].copy()
df = df.astype(object).where(pd.notna(df), None)
# print(df)
total_rows = len(df)
rows = [tuple(x) for x in df.to_numpy()]

# ==================== Upsert via temp table (no unique constraint on maintenance) ====================
updated = 0
inserted = 0
conn = engine.raw_connection()
try:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE tmp_wil_maintenance (
                date date,
                maint_ob integer,
                veh_id integer,
                maint_categ text,
                maint_loc text,
                enter_shop timestamp,
                exit_shop timestamp,
                enter_odo integer,
                exit_odo integer,
                parts_cost numeric,
                labor_cost numeric,
                add_cost numeric,
                add_cost_desc text,
                warranty boolean,
                problem text,
                work_perf text
            ) ON COMMIT DROP
        """)

        extras.execute_values(
            cur,
            """
            INSERT INTO tmp_wil_maintenance (
                date, maint_ob, veh_id, maint_categ, maint_loc,
                enter_shop, exit_shop, enter_odo, exit_odo,
                parts_cost, labor_cost, add_cost, add_cost_desc,
                warranty, problem, work_perf
            ) VALUES %s
            """,
            rows,
            page_size=1000,
        )

        # Fill NULL fields in existing rows using incoming non-NULL values.
        cur.execute("""
            UPDATE public.maintenance AS m
            SET
                date          = COALESCE(m.date, t.date),
                maint_ob      = COALESCE(m.maint_ob, t.maint_ob),
                maint_categ   = COALESCE(m.maint_categ, t.maint_categ),
                maint_loc     = COALESCE(m.maint_loc, t.maint_loc),
                exit_shop     = COALESCE(m.exit_shop, t.exit_shop),
                enter_odo     = COALESCE(m.enter_odo, t.enter_odo),
                exit_odo      = COALESCE(m.exit_odo, t.exit_odo),
                parts_cost    = COALESCE(m.parts_cost, t.parts_cost),
                labor_cost    = COALESCE(m.labor_cost, t.labor_cost),
                add_cost      = COALESCE(m.add_cost, t.add_cost),
                add_cost_desc = COALESCE(m.add_cost_desc, t.add_cost_desc),
                warranty      = COALESCE(m.warranty, t.warranty),
                problem       = COALESCE(m.problem, t.problem),
                work_perf     = COALESCE(m.work_perf, t.work_perf)
            FROM tmp_wil_maintenance AS t
            WHERE m.veh_id = t.veh_id
              AND m.enter_shop = t.enter_shop
              AND (
                    (m.date IS NULL AND t.date IS NOT NULL) OR
                    (m.maint_ob IS NULL AND t.maint_ob IS NOT NULL) OR
                    (m.maint_categ IS NULL AND t.maint_categ IS NOT NULL) OR
                    (m.maint_loc IS NULL AND t.maint_loc IS NOT NULL) OR
                    (m.exit_shop IS NULL AND t.exit_shop IS NOT NULL) OR
                    (m.enter_odo IS NULL AND t.enter_odo IS NOT NULL) OR
                    (m.exit_odo IS NULL AND t.exit_odo IS NOT NULL) OR
                    (m.parts_cost IS NULL AND t.parts_cost IS NOT NULL) OR
                    (m.labor_cost IS NULL AND t.labor_cost IS NOT NULL) OR
                    (m.add_cost IS NULL AND t.add_cost IS NOT NULL) OR
                    (m.add_cost_desc IS NULL AND t.add_cost_desc IS NOT NULL) OR
                    (m.warranty IS NULL AND t.warranty IS NOT NULL) OR
                    (m.problem IS NULL AND t.problem IS NOT NULL) OR
                    (m.work_perf IS NULL AND t.work_perf IS NOT NULL)
              )
            RETURNING m.id
        """)
        ret_upd = cur.fetchall()
        updated = len(ret_upd) if ret_upd is not None else 0

        # Insert rows that do not already exist by (veh_id, enter_shop).
        cur.execute("""
            INSERT INTO public.maintenance (
                date, maint_ob, veh_id, maint_categ, maint_loc,
                enter_shop, exit_shop, enter_odo, exit_odo,
                parts_cost, labor_cost, add_cost, add_cost_desc,
                warranty, problem, work_perf
            )
            SELECT
                t.date, t.maint_ob, t.veh_id, t.maint_categ, t.maint_loc,
                t.enter_shop, t.exit_shop, t.enter_odo, t.exit_odo,
                t.parts_cost, t.labor_cost, t.add_cost, t.add_cost_desc,
                t.warranty, t.problem, t.work_perf
            FROM tmp_wil_maintenance t
            LEFT JOIN public.maintenance m
              ON m.veh_id = t.veh_id
             AND m.enter_shop = t.enter_shop
            WHERE m.id IS NULL
            RETURNING id
        """)
        ret_ins = cur.fetchall()
        inserted = len(ret_ins) if ret_ins is not None else 0
    conn.commit()
finally:
    conn.close()

# ==================== Report ====================
attempted = total_rows
skipped_existing = attempted - inserted
print("=== Maintenance Upload Summary ===")
print(f"Rows read from file:          {total_rows}")
print(f"Rows attempted to insert:     {attempted}")
print(f"Updated (nulls filled):       {updated}")
print(f"Inserted (new):               {inserted}")
print(f"Skipped (already existed):    {skipped_existing}")
print(f"[INFO] Upserted Wilsbach maintenance rows (inserted={inserted}, updated={updated}).")
