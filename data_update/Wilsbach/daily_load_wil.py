import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2.extras as extras

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import get_conn  # noqa: E402


FOLDER_PATH = Path(
    r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Wilsbach Distributors\Daily usage"
)
FILE_NAME = r"Wilsbach EV Data Collection – Vehicle Daily Usage Summary 12-2025.xlsx"


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = (
        out.columns.astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )
    return out


def _resolve_soc(v):
    if pd.isna(v):
        return None
    try:
        x = float(v)
    except Exception:
        return None
    # Support both 0-1 and 0-100 styles.
    return x / 100.0 if x > 1 else x


def _pick_first_nonnull(s: pd.Series):
    s = s.dropna()
    return s.iloc[0] if not s.empty else None


def _pick_last_nonnull(s: pd.Series):
    s = s.dropna()
    return s.iloc[-1] if not s.empty else None


def parse_daily_file(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name="Daily Summary")
    df = _normalize_cols(raw)

    aliases = {
        "veh": ["Vehicle ID"],
        "date": ["Date"],
        "trip": ["Trip Nbr"],
        "start_odo": ["Start Odometer"],
        "end_odo": ["End Odometer"],
        "dist": ["Distance Traveled"],
        "travel_hr": ["Total Travel Time", "Total Travel Time (Hrs)"],
        "idle_hr": ["Total Idle Time", "Total Idle Time (Hrs)"],
        "init_soc": ["Initial SOC"],
        "final_soc": ["Final SOC"],
        "soc_used": ["% Used", "SOC Used"],
        "energy": ["Calc kWHh Used"],
    }

    resolved = {}
    missing = []
    for key, options in aliases.items():
        col = next((c for c in options if c in df.columns), None)
        if col is None:
            missing.append(f"{key}: {options}")
        else:
            resolved[key] = col

    if missing:
        raise RuntimeError(f"Missing expected columns in {path.name}: {missing}")

    work = pd.DataFrame(
        {
            "fleet_vehicle_id": df[resolved["veh"]].astype(str).str.strip(),
            "date": pd.to_datetime(df[resolved["date"]], errors="coerce").dt.date,
            "trip_num_row": pd.to_numeric(df[resolved["trip"]], errors="coerce"),
            "init_odo_row": pd.to_numeric(df[resolved["start_odo"]], errors="coerce"),
            "final_odo_row": pd.to_numeric(df[resolved["end_odo"]], errors="coerce"),
            "tot_dist_row": pd.to_numeric(df[resolved["dist"]], errors="coerce"),
            "tot_dura_row": pd.to_numeric(df[resolved["travel_hr"]], errors="coerce"),
            "idle_time_row": pd.to_numeric(df[resolved["idle_hr"]], errors="coerce"),
            "init_soc_row": df[resolved["init_soc"]].map(_resolve_soc),
            "final_soc_row": df[resolved["final_soc"]].map(_resolve_soc),
            "tot_soc_used_row": df[resolved["soc_used"]].map(_resolve_soc),
            "tot_energy_row": pd.to_numeric(df[resolved["energy"]], errors="coerce"),
        }
    )

    # Drop fully unusable rows.
    work = work.dropna(subset=["fleet_vehicle_id", "date"], how="any").copy()
    if work.empty:
        return pd.DataFrame()

    # Sort trips so first/last SOC and odometer can be derived deterministically.
    work = work.sort_values(["fleet_vehicle_id", "date", "trip_num_row"], na_position="last")

    # Aggregate trip rows to one row per vehicle/date for veh_daily.
    agg = (
        work.groupby(["fleet_vehicle_id", "date"], as_index=False)
        .agg(
            trip_num=("trip_num_row", "max"),
            init_odo=("init_odo_row", lambda s: _pick_first_nonnull(s)),
            final_odo=("final_odo_row", lambda s: _pick_last_nonnull(s)),
            tot_dist=("tot_dist_row", "sum"),
            tot_dura=("tot_dura_row", "sum"),
            idle_time=("idle_time_row", "sum"),
            init_soc=("init_soc_row", lambda s: _pick_first_nonnull(s)),
            final_soc=("final_soc_row", lambda s: _pick_last_nonnull(s)),
            tot_soc_used=("tot_soc_used_row", "sum"),
            tot_energy=("tot_energy_row", "sum"),
        )
    )

    # Round to stable storage precision.
    for c in ["tot_dist", "tot_dura", "idle_time", "tot_energy"]:
        agg[c] = pd.to_numeric(agg[c], errors="coerce").round(3)
    for c in ["init_soc", "final_soc", "tot_soc_used"]:
        agg[c] = pd.to_numeric(agg[c], errors="coerce").round(4)

    return agg


def main():
    file_path = FOLDER_PATH / FILE_NAME
    if not file_path.exists() or file_path.name.startswith("~$"):
        print(f"[INFO] File not found: {file_path}")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, fleet_vehicle_id FROM vehicle")
            veh_map = {str(fid): vid for vid, fid in cur.fetchall()}

        total_inserted_or_updated = 0
        total_rows = 0
        total_unmapped = 0

        path = file_path
        daily = parse_daily_file(path)
        if daily.empty:
            print(f"[INFO] {path.name}: no usable rows")
            return

        total_rows += len(daily)
        daily["veh_id"] = daily["fleet_vehicle_id"].map(veh_map)
        unmapped = int(daily["veh_id"].isna().sum())
        total_unmapped += unmapped
        daily = daily[daily["veh_id"].notna()].copy()
        if daily.empty:
            print(f"[WARN] {path.name}: all rows unmapped to vehicle table")
            return

        rows = []
        for r in daily.itertuples(index=False):
            rows.append(
                (
                    int(r.veh_id),
                    r.date,
                    int(r.trip_num) if pd.notna(r.trip_num) else None,
                    float(r.init_odo) if pd.notna(r.init_odo) else None,
                    float(r.final_odo) if pd.notna(r.final_odo) else None,
                    float(r.tot_dist) if pd.notna(r.tot_dist) else None,
                    float(r.tot_dura) if pd.notna(r.tot_dura) else None,
                    float(r.idle_time) if pd.notna(r.idle_time) else None,
                    float(r.init_soc) if pd.notna(r.init_soc) else None,
                    float(r.final_soc) if pd.notna(r.final_soc) else None,
                    float(r.tot_soc_used) if pd.notna(r.tot_soc_used) else None,
                    float(r.tot_energy) if pd.notna(r.tot_energy) else None,
                    None,  # peak_payload
                )
            )

        sql = """
        INSERT INTO public.veh_daily (
            veh_id, date, trip_num, init_odo, final_odo, tot_dist, tot_dura, idle_time,
            init_soc, final_soc, tot_soc_used, tot_energy, peak_payload
        )
        VALUES %s
        ON CONFLICT (veh_id, date) DO UPDATE SET
            trip_num     = EXCLUDED.trip_num,
            init_odo     = EXCLUDED.init_odo,
            final_odo    = EXCLUDED.final_odo,
            tot_dist     = EXCLUDED.tot_dist,
            tot_dura     = EXCLUDED.tot_dura,
            idle_time    = EXCLUDED.idle_time,
            init_soc     = EXCLUDED.init_soc,
            final_soc    = EXCLUDED.final_soc,
            tot_soc_used = EXCLUDED.tot_soc_used,
            tot_energy   = EXCLUDED.tot_energy
        WHERE
            veh_daily.trip_num     IS DISTINCT FROM EXCLUDED.trip_num OR
            veh_daily.init_odo     IS DISTINCT FROM EXCLUDED.init_odo OR
            veh_daily.final_odo    IS DISTINCT FROM EXCLUDED.final_odo OR
            veh_daily.tot_dist     IS DISTINCT FROM EXCLUDED.tot_dist OR
            veh_daily.tot_dura     IS DISTINCT FROM EXCLUDED.tot_dura OR
            veh_daily.idle_time    IS DISTINCT FROM EXCLUDED.idle_time OR
            veh_daily.init_soc     IS DISTINCT FROM EXCLUDED.init_soc OR
            veh_daily.final_soc    IS DISTINCT FROM EXCLUDED.final_soc OR
            veh_daily.tot_soc_used IS DISTINCT FROM EXCLUDED.tot_soc_used OR
            veh_daily.tot_energy   IS DISTINCT FROM EXCLUDED.tot_energy
        RETURNING 1
        """

        with conn.cursor() as cur:
            ret = extras.execute_values(cur, sql, rows, page_size=1000, fetch=True)
            changed = len(ret) if ret is not None else 0
        conn.commit()
        total_inserted_or_updated += changed
        print(
            f"[OK] {path.name}: processed={len(rows)}, changed={changed}, unmapped_dropped={unmapped}"
        )

    print("=== Wilsbach Daily Usage Upload Summary ===")
    print("Files processed:              1")
    print(f"Vehicle-date rows parsed:     {total_rows}")
    print(f"Rows inserted/updated:        {total_inserted_or_updated}")
    print(f"Rows dropped (unmapped veh):  {total_unmapped}")


if __name__ == "__main__":
    main()
