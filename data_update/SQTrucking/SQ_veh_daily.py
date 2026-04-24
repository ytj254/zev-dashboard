import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2.extras as extras

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import get_conn  # noqa: E402
from data_update.paths import INCOMING_DATA_DIR  # noqa: E402
from data_update.SQTrucking.sq_common import (  # noqa: E402
    VIN_TO_FLEET_VEHICLE_ID,
    ensure_sq_vehicles,
    load_sq_vehicle_map,
    normalize_cols,
    nullable_float,
    nullable_int,
)


FILE_PATH = (
    INCOMING_DATA_DIR
    / "SQ Trucking"
    / "daily usage"
    / "Vehicle daily usage summary_20260304_090907.xlsx"
)


def _duration_to_hours(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timedelta):
        return value.total_seconds() / 3600
    if isinstance(value, (int, float)):
        # Excel time durations can appear as fractions of a day.
        return float(value) * 24

    text = str(value).strip()
    if not text:
        return None
    td = pd.to_timedelta(text, errors="coerce")
    if pd.isna(td):
        return None
    return td.total_seconds() / 3600


def _first_nonnull(series: pd.Series):
    series = series.dropna()
    return series.iloc[0] if not series.empty else None


def _last_nonnull(series: pd.Series):
    series = series.dropna()
    return series.iloc[-1] if not series.empty else None


def parse_daily_file(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name="Report")
    df = normalize_cols(raw)

    required = [
        "Vehicle ID",
        "Trip",
        "Date",
        "Begin Odometer",
        "End Odometer",
        "Idling Duration",
        "Driving Duration",
        "Distance",
        "Beginning State Of Charge",
        "Ending State Of Charge",
        "Total kWh used",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise RuntimeError(f"Missing expected columns in {path.name}: {missing}")

    work = pd.DataFrame(
        {
            "vin": df["Vehicle ID"].astype(str).str.strip(),
            "date": pd.to_datetime(df["Date"], errors="coerce").dt.date,
            "trip_num_row": pd.to_numeric(df["Trip"], errors="coerce"),
            "init_odo_row": pd.to_numeric(df["Begin Odometer"], errors="coerce"),
            "final_odo_row": pd.to_numeric(df["End Odometer"], errors="coerce"),
            "idle_time_row": df["Idling Duration"].map(_duration_to_hours),
            "tot_dura_row": df["Driving Duration"].map(_duration_to_hours),
            "tot_dist_row": pd.to_numeric(df["Distance"], errors="coerce"),
            "init_soc_row": pd.to_numeric(df["Beginning State Of Charge"], errors="coerce"),
            "final_soc_row": pd.to_numeric(df["Ending State Of Charge"], errors="coerce"),
            "tot_energy_row": pd.to_numeric(df["Total kWh used"], errors="coerce"),
        }
    )
    work = work.dropna(subset=["vin", "date"]).copy()
    work = work[work["vin"].ne("")].copy()
    if work.empty:
        return pd.DataFrame()

    work["soc_used_row"] = (work["init_soc_row"] - work["final_soc_row"]).clip(lower=0)
    work = work.sort_values(["vin", "date", "trip_num_row"], na_position="last")

    daily = (
        work.groupby(["vin", "date"], as_index=False)
        .agg(
            trip_num=("trip_num_row", "max"),
            init_odo=("init_odo_row", _first_nonnull),
            final_odo=("final_odo_row", _last_nonnull),
            tot_dist=("tot_dist_row", "sum"),
            tot_dura=("tot_dura_row", "sum"),
            idle_time=("idle_time_row", "sum"),
            init_soc=("init_soc_row", _first_nonnull),
            final_soc=("final_soc_row", _last_nonnull),
            tot_soc_used=("soc_used_row", "sum"),
            tot_energy=("tot_energy_row", "sum"),
        )
    )

    daily["fleet_vehicle_id"] = daily["vin"].map(VIN_TO_FLEET_VEHICLE_ID)
    for col in ["tot_dist", "tot_dura", "idle_time", "tot_energy"]:
        daily[col] = pd.to_numeric(daily[col], errors="coerce").round(3)
    for col in ["init_soc", "final_soc", "tot_soc_used"]:
        daily[col] = pd.to_numeric(daily[col], errors="coerce").round(4)

    return daily


def upload_daily(conn, daily: pd.DataFrame) -> int:
    rows = []
    for r in daily.itertuples(index=False):
        rows.append(
            (
                int(r.veh_id),
                r.date,
                nullable_int(r.trip_num),
                nullable_float(r.init_odo),
                nullable_float(r.final_odo),
                nullable_float(r.tot_dist),
                nullable_float(r.tot_dura),
                nullable_float(r.idle_time),
                nullable_float(r.init_soc),
                nullable_float(r.final_soc),
                nullable_float(r.tot_soc_used),
                nullable_float(r.tot_energy),
                None,
            )
        )

    sql = """
        INSERT INTO public.veh_daily (
            veh_id, date, trip_num, init_odo, final_odo, tot_dist, tot_dura, idle_time,
            init_soc, final_soc, tot_soc_used, tot_energy, peak_payload
        )
        VALUES %s
        ON CONFLICT (veh_id, date) DO UPDATE SET
            trip_num = EXCLUDED.trip_num,
            init_odo = EXCLUDED.init_odo,
            final_odo = EXCLUDED.final_odo,
            tot_dist = EXCLUDED.tot_dist,
            tot_dura = EXCLUDED.tot_dura,
            idle_time = EXCLUDED.idle_time,
            init_soc = EXCLUDED.init_soc,
            final_soc = EXCLUDED.final_soc,
            tot_soc_used = EXCLUDED.tot_soc_used,
            tot_energy = EXCLUDED.tot_energy
        WHERE
            veh_daily.trip_num IS DISTINCT FROM EXCLUDED.trip_num OR
            veh_daily.init_odo IS DISTINCT FROM EXCLUDED.init_odo OR
            veh_daily.final_odo IS DISTINCT FROM EXCLUDED.final_odo OR
            veh_daily.tot_dist IS DISTINCT FROM EXCLUDED.tot_dist OR
            veh_daily.tot_dura IS DISTINCT FROM EXCLUDED.tot_dura OR
            veh_daily.idle_time IS DISTINCT FROM EXCLUDED.idle_time OR
            veh_daily.init_soc IS DISTINCT FROM EXCLUDED.init_soc OR
            veh_daily.final_soc IS DISTINCT FROM EXCLUDED.final_soc OR
            veh_daily.tot_soc_used IS DISTINCT FROM EXCLUDED.tot_soc_used OR
            veh_daily.tot_energy IS DISTINCT FROM EXCLUDED.tot_energy
        RETURNING 1
    """
    with conn.cursor() as cur:
        returned = extras.execute_values(cur, sql, rows, page_size=1000, fetch=True)
    return len(returned) if returned is not None else 0


def main():
    if not FILE_PATH.exists():
        raise FileNotFoundError(FILE_PATH)

    daily = parse_daily_file(FILE_PATH)
    if daily.empty:
        print(f"[INFO] No usable rows found in {FILE_PATH.name}")
        return

    unmapped_vins = sorted(daily.loc[daily["fleet_vehicle_id"].isna(), "vin"].unique())
    if unmapped_vins:
        raise RuntimeError(f"Unmapped VINs: {unmapped_vins}")

    with get_conn() as conn:
        ensure_sq_vehicles(conn)
        vehicle_map = load_sq_vehicle_map(conn)

        daily["veh_id"] = daily["fleet_vehicle_id"].map(vehicle_map)
        missing_db_ids = sorted(daily.loc[daily["veh_id"].isna(), "fleet_vehicle_id"].unique())
        if missing_db_ids:
            raise RuntimeError(f"Fleet vehicle IDs missing from database: {missing_db_ids}")

        changed = upload_daily(conn, daily)
        conn.commit()

    print("=== SQ Trucking Daily Usage Upload Summary ===")
    print(f"File:                         {FILE_PATH.name}")
    print(f"Vehicle-date rows parsed:     {len(daily)}")
    print(f"Date range:                   {daily['date'].min()} to {daily['date'].max()}")
    print(f"Vehicles in file:             {', '.join(sorted(daily['fleet_vehicle_id'].unique()))}")
    print(f"Rows inserted/updated:        {changed}")


if __name__ == "__main__":
    main()
