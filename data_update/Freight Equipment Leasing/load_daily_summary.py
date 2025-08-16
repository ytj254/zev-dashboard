from pathlib import Path
import pandas as pd
import psycopg2.extras as _extras
from data_update.common_data_update import get_conn
from common import (
    ROOT_DIR, FREIGHT_VEH_IDS, md5_file, already_ingested,
    record_ingestion, list_date_subfolders, is_monthly_folder,
    normalize_soc, minutes_to_hours, round_int,
    get_fleet_id_and_vehicle_maps
)

EXCEL_NAME = "AO_Daily_Summary.xlsx"

EXPECTED_COLS = [
    "Day",
    "Total Trips in Day",
    "Initial Odometer Reading",
    "Final Odometer Reading",
    "Total Daily Distance Driven (miles)",
    "Total Daily Drive Duration (minutes)",
    "Idle Time (minutes)",
    "Initial SOC",
    "Final SOC",
    "Total SOC Used",
    "Total Energy Consumed for the Day (kWh)"
]

def parse_vehicle_sheet(sheet_df: pd.DataFrame) -> pd.DataFrame:
    # Remove completely empty rows
    df = sheet_df.dropna(how="all")
    # print(df)

    df.columns = df.columns.str.strip()  # remove leading/trailing spaces
    df.columns = df.columns.str.replace('\n', ' ', regex=True)  # replace line breaks with space
    df.columns = df.columns.str.replace('\s+', ' ', regex=True)  # collapse multiple spaces

    # Remove "Grand Total" row if present
    if "Day" in df.columns:
        df = df[df["Day"].astype(str).str.lower() != "grand total"]
        
    # print(df)

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["Day"], errors="coerce").dt.date
    out["trip_num"] = pd.to_numeric(df["Total Trips in Day"], errors="coerce").astype("Int64")
    out["init_odo"] = pd.to_numeric(df["Initial Odometer Reading"], errors="coerce")
    out["final_odo"] = pd.to_numeric(df["Final Odometer Reading"], errors="coerce")
    out["tot_dist"] = pd.to_numeric(df["Total Daily Distance Driven (miles)"], errors="coerce").round(2)

    # Convert minutes to hours
    out["tot_dura"] = pd.to_numeric(df["Total Daily Drive Duration (minutes)"], errors="coerce").map(minutes_to_hours)
    out["idle_time"] = pd.to_numeric(df["Idle Time (minutes)"], errors="coerce").map(minutes_to_hours)

    # Normalize SOC
    out["init_soc"] = df["Initial SOC"].map(normalize_soc)
    out["final_soc"] = df["Final SOC"].map(normalize_soc)
    out["tot_soc_used"] = df["Total SOC Used"].map(normalize_soc)

    out["tot_energy"] = pd.to_numeric(df["Total Energy Consumed for the Day (kWh)"], errors="coerce").map(round_int)
    # print(out)
    return out

def upsert_daily(conn, veh_id_int: int, df: pd.DataFrame) -> int:
    rows = []
    warns = 0
    for _, r in df.iterrows():
        if pd.isna(r["date"]):
            warns += 1
        rows.append((
            veh_id_int,
            r.get("date", None),
            int(r["trip_num"]) if pd.notna(r["trip_num"]) else None,
            r["init_odo"] if pd.notna(r["init_odo"]) else None,
            r["final_odo"] if pd.notna(r["final_odo"]) else None,
            r["tot_dist"] if pd.notna(r["tot_dist"]) else None,
            r["tot_dura"] if pd.notna(r["tot_dura"]) else None,
            r["idle_time"] if pd.notna(r["idle_time"]) else None,
            r.get("init_soc"),
            r.get("final_soc"),
            r.get("tot_soc_used"),
            int(r["tot_energy"]) if pd.notna(r["tot_energy"]) else None,
            None  # peak_payload is always None here
        ))
    if warns:
        print(f"[WARN] veh_id={veh_id_int}: {warns} rows missing date; inserted with NULL date.")

    with conn.cursor() as cur:
        _extras.execute_values(cur, """
            INSERT INTO public.veh_daily
            (veh_id, date, trip_num, init_odo, final_odo, tot_dist, tot_dura, idle_time,
             init_soc, final_soc, tot_soc_used, tot_energy, peak_payload)
            VALUES %s
            ON CONFLICT (veh_id, date) DO NOTHING
        """, rows, page_size=500)
    return len(rows)

def main():
    root = Path(ROOT_DIR)
    excel_files = [
        folder / EXCEL_NAME
        for folder in list_date_subfolders(root)
        if is_monthly_folder(folder)
    ]

    if not excel_files:
        print("No monthly Excel files found.")
        return

    with get_conn() as conn:
        fleet_id, veh_map = get_fleet_id_and_vehicle_maps(conn)
        # arc = ensure_archive(root)
        total = 0

        for xls in sorted(excel_files):
            file_hash = md5_file(xls)
            if already_ingested(conn, xls, file_hash):
                print(f"[SKIP] {xls.name} already ingested.")
                continue

            rows_loaded = 0
            # ✅ Use context manager to avoid locking the file
            with pd.ExcelFile(xls, engine="openpyxl") as xl:
                for sheet in xl.sheet_names:
                    if sheet not in FREIGHT_VEH_IDS:
                        continue
                    if sheet not in veh_map:
                        raise RuntimeError(f"Vehicle {sheet} not found in DB map")
                    veh_id_int = veh_map[sheet]
                    df_sheet = xl.parse(sheet_name=sheet, header=2)
                    parsed = parse_vehicle_sheet(df_sheet)
                    rows_loaded += upsert_daily(conn, veh_id_int, parsed)

            record_ingestion(conn, xls, file_hash, rows_loaded)
            # move_to_archive(xls, arc)  # No more lock here
            total += rows_loaded
            print(f"[OK] {xls.name}: {rows_loaded} rows loaded and archived.")

        print(f"Done. Total daily rows: {total}")

if __name__ == "__main__":
    main()
