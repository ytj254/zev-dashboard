import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2.extras as extras

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import get_conn  # noqa: E402
from data_update.paths import INCOMING_DATA_DIR  # noqa: E402
from data_update.SQTrucking.common_sq import (  # noqa: E402
    VIN_TO_FLEET_VEHICLE_ID,
    ensure_sq_vehicles,
    load_sq_vehicle_map,
    normalize_cols,
    normalize_soc,
    nullable_float,
)


FILE_PATH = (
    INCOMING_DATA_DIR
    / "SQ Trucking"
    / "telematics"
    / "Custom Vehicle Dataset Report V2_20260303_100321.xlsx"
)
REPORT_SHEET = "Report"
REPORT_HEADER_ROW = 12
LOCAL_TIMEZONE = "America/New_York"


def parse_telematics_file(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=REPORT_SHEET, header=REPORT_HEADER_ROW)
    df = normalize_cols(raw)

    required = [
        "Vehicle ID",
        "Date/Time",
        "Latitude",
        "Longitude",
        "Speed (mph)",
        "State of Charge",
        "Odometer",
        "Details",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise RuntimeError(f"Missing expected columns in {path.name}: {missing}")

    timestamp_naive = pd.to_datetime(df["Date/Time"], errors="coerce")
    timestamp_local = timestamp_naive.dt.tz_localize(
        LOCAL_TIMEZONE,
        ambiguous="NaT",
        nonexistent="shift_forward",
    )

    out = pd.DataFrame(
        {
            "vin": df["Vehicle ID"].astype(str).str.strip(),
            "timestamp": timestamp_local.dt.tz_convert("UTC"),
            "speed": pd.to_numeric(df["Speed (mph)"], errors="coerce"),
            "mileage": pd.to_numeric(df["Odometer"], errors="coerce"),
            "soc": df["State of Charge"].map(normalize_soc),
            "latitude": pd.to_numeric(df["Latitude"], errors="coerce"),
            "longitude": pd.to_numeric(df["Longitude"], errors="coerce"),
            "details": df["Details"].astype(str),
        }
    )
    out["fleet_vehicle_id"] = out["vin"].map(VIN_TO_FLEET_VEHICLE_ID)
    out["elevation"] = None
    out["key_on_time"] = None

    return out


def clean_telematics(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    counts = {
        "rows_read": len(df),
        "missing_timestamp": int(df["timestamp"].isna().sum()),
        "unmapped_vehicle": int(df["fleet_vehicle_id"].isna().sum()),
        "bad_speed": int((df["speed"] < 0).sum()),
        "bad_soc": int(((df["soc"] < 0) | (df["soc"] > 1)).sum()),
        "bad_mileage": int((df["mileage"] < 0).sum()),
        "invalid_gps_nulled": 0,
        "duplicates_removed": 0,
    }

    keep = (
        df["timestamp"].notna()
        & df["fleet_vehicle_id"].notna()
        & ~(df["speed"] < 0)
        & ~((df["soc"] < 0) | (df["soc"] > 1))
        & ~(df["mileage"] < 0)
    )
    work = df.loc[keep].copy()

    invalid_gps = ~work["latitude"].between(-90, 90) | ~work["longitude"].between(-180, 180)
    counts["invalid_gps_nulled"] = int(invalid_gps.sum())
    if counts["invalid_gps_nulled"]:
        work.loc[invalid_gps, ["latitude", "longitude"]] = np.nan

    details_lower = work["details"].str.lower()
    work["priority"] = 0
    work.loc[~details_lower.str.contains("ignition", na=False), "priority"] += 2
    work.loc[work["speed"].fillna(0) > 0, "priority"] += 4
    work.loc[work["latitude"].notna() & work["longitude"].notna(), "priority"] += 1
    work["speed_sort"] = work["speed"].fillna(-1)

    before = len(work)
    work = (
        work.sort_values(["fleet_vehicle_id", "timestamp", "priority", "speed_sort"])
        .drop_duplicates(["fleet_vehicle_id", "timestamp"], keep="last")
        .drop(columns=["priority", "speed_sort"])
    )
    counts["duplicates_removed"] = before - len(work)

    return work, counts


def upload_telematics(conn, df: pd.DataFrame) -> int:
    rows = []
    for r in df.itertuples(index=False):
        rows.append(
            (
                int(r.veh_id),
                r.timestamp.to_pydatetime(),
                nullable_float(r.elevation),
                nullable_float(r.speed),
                nullable_float(r.mileage),
                nullable_float(r.soc),
                nullable_float(r.key_on_time),
                nullable_float(r.latitude),
                nullable_float(r.longitude),
                nullable_float(r.longitude),
                nullable_float(r.latitude),
                nullable_float(r.longitude),
                nullable_float(r.latitude),
            )
        )

    sql = """
        INSERT INTO public.veh_tel (
            veh_id, "timestamp", elevation, speed, mileage, soc, key_on_time,
            latitude, longitude, location
        )
        VALUES %s
        ON CONFLICT (veh_id, "timestamp") DO UPDATE SET
            elevation = EXCLUDED.elevation,
            speed = EXCLUDED.speed,
            mileage = EXCLUDED.mileage,
            soc = EXCLUDED.soc,
            key_on_time = EXCLUDED.key_on_time,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            location = EXCLUDED.location
        WHERE
            veh_tel.elevation IS DISTINCT FROM EXCLUDED.elevation OR
            veh_tel.speed IS DISTINCT FROM EXCLUDED.speed OR
            veh_tel.mileage IS DISTINCT FROM EXCLUDED.mileage OR
            veh_tel.soc IS DISTINCT FROM EXCLUDED.soc OR
            veh_tel.key_on_time IS DISTINCT FROM EXCLUDED.key_on_time OR
            veh_tel.latitude IS DISTINCT FROM EXCLUDED.latitude OR
            veh_tel.longitude IS DISTINCT FROM EXCLUDED.longitude OR
            veh_tel.location IS DISTINCT FROM EXCLUDED.location
        RETURNING 1
    """
    template = """
        (%s, %s, %s, %s, %s, %s, %s, %s, %s,
         CASE
            WHEN %s IS NOT NULL AND %s IS NOT NULL
            THEN public.ST_SetSRID(public.ST_MakePoint(%s, %s), 4326)::public.geography
            ELSE NULL
         END)
    """
    with conn.cursor() as cur:
        returned = extras.execute_values(cur, sql, rows, template=template, page_size=5000, fetch=True)
    return len(returned) if returned is not None else 0


def main():
    if not FILE_PATH.exists():
        raise FileNotFoundError(FILE_PATH)

    parsed = parse_telematics_file(FILE_PATH)
    unmapped_vins = sorted(parsed.loc[parsed["fleet_vehicle_id"].isna(), "vin"].dropna().unique())
    if unmapped_vins:
        raise RuntimeError(f"Unmapped VINs: {unmapped_vins}")

    cleaned, counts = clean_telematics(parsed)
    if cleaned.empty:
        print(f"[INFO] No usable telematics rows found in {FILE_PATH.name}")
        return

    with get_conn() as conn:
        ensure_sq_vehicles(conn)
        vehicle_map = load_sq_vehicle_map(conn)
        cleaned["veh_id"] = cleaned["fleet_vehicle_id"].map(vehicle_map)

        missing_db_ids = sorted(cleaned.loc[cleaned["veh_id"].isna(), "fleet_vehicle_id"].unique())
        if missing_db_ids:
            raise RuntimeError(f"Fleet vehicle IDs missing from database: {missing_db_ids}")

        uploaded = upload_telematics(conn, cleaned)
        conn.commit()

    print("=== SQ Trucking Telematics Upload Summary ===")
    print(f"File:                         {FILE_PATH.name}")
    print(f"Rows read:                    {counts['rows_read']}")
    print(f"Dropped missing timestamp:    {counts['missing_timestamp']}")
    print(f"Dropped unmapped vehicle:     {counts['unmapped_vehicle']}")
    print(f"Dropped bad speed:            {counts['bad_speed']}")
    print(f"Dropped bad SOC:              {counts['bad_soc']}")
    print(f"Dropped bad mileage:          {counts['bad_mileage']}")
    print(f"Invalid GPS nulled:           {counts['invalid_gps_nulled']}")
    print(f"Duplicates removed:           {counts['duplicates_removed']}")
    print(f"Rows attempted:               {len(cleaned)}")
    print(f"Timestamp range UTC:          {cleaned['timestamp'].min()} to {cleaned['timestamp'].max()}")
    print(f"Vehicles in file:             {', '.join(sorted(cleaned['fleet_vehicle_id'].unique()))}")
    print(f"Rows inserted/updated:        {uploaded}")


if __name__ == "__main__":
    main()
