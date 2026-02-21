"""
Compute daily vehicle usage from telematics and upsert veh_daily rows (overwrite existing).

Defaults:
    - Fleets: 2 Watsontown Trucking
    - Idle gap threshold: 15 minutes (used to separate trips)
"""

import argparse
import datetime as dt
import sys
from typing import List, Tuple, Dict, Any

import os
# Allow running the script directly without installing the package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from psycopg2.extras import execute_batch
from data_update.common_data_update import get_conn


def log(msg: str) -> None:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fleet-ids",
        type=int,
        nargs="+",
        default=[2],
        help="Fleet IDs to process (default: 2).",
    )
    parser.add_argument(
        "--idle-threshold-minutes",
        type=float,
        default=15.0,
        help=(
            "Idle-time threshold (minutes) between moving periods used to "
            "split trips. Stops shorter than this stay in the same trip."
        ),
    )
    return parser.parse_args()


def fetch_telematics(cur, fleet_ids: List[int]):
    sql = """
        SELECT
            v.id AS vehicle_pk,
            v.fleet_vehicle_id,
            v.fleet_id,
            vt."timestamp",
            vt.mileage,
            vt.soc,
            vt.speed
        FROM veh_tel vt
        JOIN vehicle v ON vt.veh_id = v.id
        WHERE v.fleet_id = ANY(%s)
        ORDER BY v.id, vt."timestamp";
    """
    cur.execute(sql, (fleet_ids,))
    return cur.fetchall()


def aggregate_daily(rows, idle_threshold_minutes: float) -> Dict[Tuple[int, dt.date], Dict[str, Any]]:
    idle_threshold_sec = idle_threshold_minutes * 60.0

    grouped: Dict[Tuple[int, dt.date], List[Tuple[dt.datetime, Any, Any, Any]]] = {}
    for vehicle_pk, fleet_veh_id, fleet_id, ts, mileage, soc, speed in rows:
        if ts is None:
            continue
        key = (vehicle_pk, ts.date())
        grouped.setdefault(key, []).append((ts, mileage, soc, speed))

    aggregates: Dict[Tuple[int, dt.date], Dict[str, Any]] = {}

    for key, day_rows in grouped.items():
        veh_pk, day = key
        day_rows.sort(key=lambda r: r[0])

        _, init_odo, init_soc_val, _ = day_rows[0]
        _, final_odo, final_soc_val, _ = day_rows[-1]

        init_soc = next((float(s) for (_, _, s, _) in day_rows if s is not None), None)
        final_soc = next((float(s) for (_, _, s, _) in reversed(day_rows) if s is not None), None)

        tot_dist = (
            round(float(final_odo - init_odo), 2)
            if init_odo is not None and final_odo is not None else None
        )

        moving_indices = [i for i, (_, _, _, speed) in enumerate(day_rows) if speed is not None and speed > 0]

        if not moving_indices:
            tot_dura_hours = 0.0
            idle_hours = 0.0
            trip_num = 0
        else:
            first_m_idx = moving_indices[0]
            last_m_idx = moving_indices[-1]

            drive_sec = 0.0
            idle_sec = 0.0
            trip_num = 1
            prev_moving = False
            accumulated_stop = 0.0

            for i in range(first_m_idx, last_m_idx):
                ts_cur, _, _, speed_cur = day_rows[i]
                ts_next, _, _, _ = day_rows[i + 1]
                dt_sec = (ts_next - ts_cur).total_seconds()
                if dt_sec <= 0:
                    continue

                moving = speed_cur is not None and speed_cur > 0

                if moving:
                    drive_sec += dt_sec
                    if not prev_moving and accumulated_stop >= idle_threshold_sec:
                        trip_num += 1
                    # Reset stop accumulator whenever movement resumes so
                    # separate short stops do not incorrectly accumulate.
                    accumulated_stop = 0.0
                    prev_moving = True
                else:
                    idle_sec += dt_sec
                    accumulated_stop += dt_sec
                    prev_moving = False

            tot_dura_hours = round(drive_sec / 3600.0, 2)
            idle_hours = round(idle_sec / 3600.0, 2)

        tot_soc_used = None
        if init_soc is not None and final_soc is not None:
            # Daily "SOC used" should not be negative; clamp net gain days to 0.
            tot_soc_used = round(max(init_soc - final_soc, 0.0), 4)

        aggregates[key] = {
            "veh_id": veh_pk,
            "date": day,
            "init_odo": init_odo,
            "final_odo": final_odo,
            "tot_dist": tot_dist,
            "tot_dura": tot_dura_hours,
            "idle_time": idle_hours,
            "init_soc": init_soc,
            "final_soc": final_soc,
            "tot_soc_used": tot_soc_used,
            "trip_num": trip_num,
            "tot_energy": None,
            "peak_payload": None,
        }

    return aggregates


def build_daily_records(
    aggregates: Dict[Tuple[int, dt.date], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return [
        {
            "veh_id": agg["veh_id"],
            "date": agg["date"],
            "trip_num": agg["trip_num"],
            "init_odo": agg["init_odo"],
            "final_odo": agg["final_odo"],
            "tot_dist": agg["tot_dist"],
            "tot_dura": agg["tot_dura"],
            "idle_time": agg["idle_time"],
            "init_soc": agg["init_soc"],
            "final_soc": agg["final_soc"],
            "tot_soc_used": agg["tot_soc_used"],
            "tot_energy": agg["tot_energy"],
            "peak_payload": agg["peak_payload"],
        }
        for agg in aggregates.values()
    ]


def insert_daily(cur, daily_records: List[Dict[str, Any]]):
    if not daily_records:
        log("No veh_daily rows to upsert.")
        return

    sql = """
        INSERT INTO veh_daily (
            veh_id,
            date,
            trip_num,
            init_odo,
            final_odo,
            tot_dist,
            tot_dura,
            idle_time,
            init_soc,
            final_soc,
            tot_soc_used,
            tot_energy,
            peak_payload
        )
        VALUES (
            %(veh_id)s,
            %(date)s,
            %(trip_num)s,
            %(init_odo)s,
            %(final_odo)s,
            %(tot_dist)s,
            %(tot_dura)s,
            %(idle_time)s,
            %(init_soc)s,
            %(final_soc)s,
            %(tot_soc_used)s,
            %(tot_energy)s,
            %(peak_payload)s
        )
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
            tot_energy   = EXCLUDED.tot_energy,
            peak_payload = EXCLUDED.peak_payload;
    """
    execute_batch(cur, sql, daily_records, page_size=1000)
    log(f"Upserted {len(daily_records)} veh_daily rows.")


def main():
    args = parse_args()

    log(f"Start veh_daily build | Fleets={args.fleet_ids} | idle_threshold={args.idle_threshold_minutes} min")

    with get_conn() as conn:
        with conn.cursor() as cur:
            rows = fetch_telematics(cur, args.fleet_ids)
            if not rows:
                log("No telematics found for given fleets. Exit.")
                return

            log(f"Fetched {len(rows)} telematics records.")

            aggregates = aggregate_daily(rows, args.idle_threshold_minutes)
            log(f"Aggregated into {len(aggregates)} vehicle-day entries.")

            dates = [d for (_, d) in aggregates.keys()]
            if dates:
                log(f"Date range in telematics for these fleets: {min(dates)} to {max(dates)}")

            records = build_daily_records(aggregates)
            log(f"Vehicle-day rows to upsert: {len(records)}")

            insert_daily(cur, records)
        conn.commit()

    log("veh_daily build completed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"ERROR: {e}")
        sys.exit(1)
