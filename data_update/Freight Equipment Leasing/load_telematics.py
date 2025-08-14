from pathlib import Path
import pandas as pd
import psycopg2.extras as _extras
from common import (
    ROOT_DIR, FREIGHT_VEH_IDS, md5_file, already_ingested,
    record_ingestion, list_date_subfolders, is_weekly_folder,
    get_conn, ensure_archive, move_to_archive,
    normalize_soc, minutes_to_hours, get_fleet_id_and_vehicle_maps,
    DATEFOLDER_RE
)

def resolve_vehicle_id(file_name: str) -> str | None:
    stem = Path(file_name).stem
    return stem if stem in FREIGHT_VEH_IDS else None

def upsert_tel(conn, veh_id_int: int, df: pd.DataFrame, veh_label: str) -> tuple[int,int]:
    rows = []
    warns = 0
    for _, r in df.iterrows():
        ts = r.get("timeStamp")
        if pd.isna(ts):
            warns += 1
        lat = r.get("latitude")
        lon = r.get("longitude")

        rows.append((
            veh_id_int,
            pd.to_datetime(ts, errors="coerce"),
            None,  # elevation (unknown)
            int(r["speed"]) if pd.notna(r.get("speed")) else None,
            int(r["odometer"]) if pd.notna(r.get("odometer")) else None,
            r.get("stateOfCharge"),
            r.get("keyOnTime"),
            float(lat) if pd.notna(lat) else None,
            float(lon) if pd.notna(lon) else None
        ))
    if warns:
        print(f"[WARN] {veh_label}: {warns} rows missing timestamp; inserted with NULL timestamp.")

    with conn.cursor() as cur:
        # Insert core columns first
        _extras.execute_values(cur, """
            INSERT INTO public.veh_tel
            (veh_id, "timestamp", elevation, speed, mileage, soc, key_on_time, latitude, longitude)
            VALUES %s
            ON CONFLICT (veh_id, "timestamp") DO NOTHING
        """, rows, page_size=1000)

        # Populate geography point for rows that have lat/lon but NULL location
        cur.execute("""
            UPDATE public.veh_tel
            SET location = CASE
                WHEN latitude IS NOT NULL AND longitude IS NOT NULL
                THEN public.ST_SetSRID(public.ST_MakePoint(longitude, latitude), 4326)::public.geography
                ELSE NULL
            END
            WHERE veh_id = %s
              AND location IS NULL
        """, (veh_id_int,))
    return len(rows), warns

def load_csv_file(conn, p: Path, str2int: dict) -> tuple[int, int]:
    veh_label = resolve_vehicle_id(p.name)
    if not veh_label:
        print(f"[SKIP] {p.name} not a whitelisted vehicle.")
        return 0,0

    df = pd.read_csv(p)

    # Ensure only expected columns are processed
    expected_cols = {"name", "timeStamp", "latitude", "longitude", "speed", 
                     "odometer", "stateOfCharge", "keyOnTime"}
    missing = expected_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing columns in {p.name}: {missing}")

    # Normalize SOC, keep keyOnTime as-is
    df["stateOfCharge"] = df["stateOfCharge"].map(normalize_soc)
    df["keyOnTime"] = pd.to_numeric(df["keyOnTime"], errors="coerce")

    veh_id_int = str2int[veh_label]
    return upsert_tel(conn, veh_id_int, df, veh_label)

def main():
    root = Path(ROOT_DIR)
    weekly_folders = [f for f in (p for p in root.iterdir() if p.is_dir()) if DATEFOLDER_RE.match(f.name)]
    weekly_folders = [f for f in weekly_folders if is_weekly_folder(f)]

    if not weekly_folders:
        print("No weekly CSV folders found.")
        return

    with get_conn() as conn:
        _, str2int = get_fleet_id_and_vehicle_maps(conn)
        arc = ensure_archive(root)
        grand_rows = grand_warns = 0

        for folder in sorted(weekly_folders):
            for csvf in sorted(folder.glob("*.csv")):
                file_hash = md5_file(csvf)
                if already_ingested(conn, csvf, file_hash):
                    print(f"[SKIP] {csvf.name} already ingested.")
                    continue
                rows, warns = load_csv_file(conn, csvf, str2int)
                record_ingestion(conn, csvf, file_hash, rows)
                move_to_archive(csvf, arc)
                grand_rows += rows
                grand_warns += warns
                print(f"[OK] {csvf.name}: {rows} rows loaded; archived.")

        print(f"Done. Tel rows: {grand_rows}. Warnings (missing timestamps): {grand_warns}")

if __name__ == "__main__":
    main()
