from pathlib import Path
import pandas as pd
import psycopg2.extras as extras
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from data_update.common_data_update import get_conn
from data_update.Freight_Equipment_Leasing.common import (
    FLEET_NAME,
    get_fleet_id_and_vehicle_maps,
    get_charger_map,
    normalize_soc,
)

# --- Config ---
EXCEL_FILE = Path(
    r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Freight Equipment Leasing\Charging log\PITT OHIO Charging Sessions List 2_2_2026.xlsx"
)
SESSIONS_SHEET = "Sessions list"
LOCAL_TZ = "America/New_York"

# Reference SQL for manual cleanup step (run before this script):
# DELETE FROM public.refuel_inf r
# WHERE r.charger_id IN (
#   SELECT c.id
#   FROM public.charger c
#   JOIN public.fleet f ON f.id = c.fleet_id
#   WHERE f.fleet_name = 'Freight Equipment Leasing'
# );


INSERT_SQL = """
INSERT INTO public.refuel_inf (
    charger_id,
    veh_id,
    connect_time,
    disconnect_time,
    avg_power,
    max_power,
    tot_energy,
    start_soc,
    end_soc,
    tot_ref_dura
) VALUES %s
ON CONFLICT ON CONSTRAINT uq_refuel_session
DO UPDATE SET
  disconnect_time = EXCLUDED.disconnect_time,
  avg_power       = EXCLUDED.avg_power,
  max_power       = EXCLUDED.max_power,
  tot_energy      = EXCLUDED.tot_energy,
  start_soc       = EXCLUDED.start_soc,
  end_soc         = EXCLUDED.end_soc,
  tot_ref_dura    = EXCLUDED.tot_ref_dura,
  veh_id          = EXCLUDED.veh_id,
  connect_time    = EXCLUDED.connect_time
WHERE
  refuel_inf.disconnect_time IS DISTINCT FROM EXCLUDED.disconnect_time OR
  refuel_inf.avg_power       IS DISTINCT FROM EXCLUDED.avg_power       OR
  refuel_inf.max_power       IS DISTINCT FROM EXCLUDED.max_power       OR
  refuel_inf.tot_energy      IS DISTINCT FROM EXCLUDED.tot_energy      OR
  refuel_inf.start_soc       IS DISTINCT FROM EXCLUDED.start_soc       OR
  refuel_inf.end_soc         IS DISTINCT FROM EXCLUDED.end_soc         OR
  refuel_inf.tot_ref_dura    IS DISTINCT FROM EXCLUDED.tot_ref_dura    OR
  refuel_inf.veh_id          IS DISTINCT FROM EXCLUDED.veh_id          OR
  refuel_inf.connect_time    IS DISTINCT FROM EXCLUDED.connect_time;
"""


def to_utc_naive(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    dt = dt.dt.tz_localize(LOCAL_TZ, ambiguous="NaT", nonexistent="NaT")
    return dt.dt.tz_convert("UTC").dt.tz_localize(None)


def parse_duration_minutes(series: pd.Series) -> pd.Series:
    td = pd.to_timedelta(series, errors="coerce")
    return (td.dt.total_seconds() / 60.0).round(2)


def parse_vehicle_from_id(series: pd.Series) -> pd.Series:
    # "DSE177 :sha256:..." -> "DSE177"
    out = series.astype(str).str.split(":", n=1).str[0].str.strip()
    return out.where(~out.eq("nan"), None)


def load_inputs(path: Path) -> pd.DataFrame:
    sessions = pd.read_excel(path, sheet_name=SESSIONS_SHEET)
    sessions.columns = sessions.columns.str.strip()
    return sessions


def main() -> None:
    if not EXCEL_FILE.exists():
        raise FileNotFoundError(f"Charging file not found: {EXCEL_FILE}")

    sessions = load_inputs(EXCEL_FILE)

    with get_conn() as conn:
        fleet_id, veh_map = get_fleet_id_and_vehicle_maps(conn)
        charger_map = get_charger_map(conn, fleet_id)

    total_rows = len(sessions)

    # Vehicle ID in Sessions list is already pre-resolved by fleet.
    sessions["fleet_vehicle_id_resolved"] = parse_vehicle_from_id(sessions["Vehicle ID"])

    # Keep only connectors defined for this fleet (drops CCS 1 / CSS 1).
    sessions["connector_norm"] = sessions["Connector"].astype(str).str.strip()
    sessions["charger_id"] = sessions["connector_norm"].map(charger_map).astype("Int64")
    sessions = sessions[sessions["charger_id"].notna()].copy()
    print(f"[INFO] Dropped {total_rows - len(sessions)} rows with invalid connector for {FLEET_NAME}")

    # Keep only rows with valid fleet vehicle mapping.
    before_vehicle = len(sessions)
    sessions["veh_id"] = sessions["fleet_vehicle_id_resolved"].map(veh_map).astype("Int64")
    sessions = sessions[sessions["veh_id"].notna()].copy()
    print(f"[INFO] Dropped {before_vehicle - len(sessions)} rows with unresolved vehicle ID")

    # Convert local timestamps to UTC before insert.
    connect_utc = to_utc_naive(sessions["Session start (America/New_York)"])
    disconnect_utc = to_utc_naive(sessions["Session end (America/New_York)"])

    duration_min = parse_duration_minutes(sessions["Charging duration (hh:mm:ss)"])
    energy_kwh = pd.to_numeric(sessions["Charged Energy (kWh)"], errors="coerce")
    peak_power_kw = pd.to_numeric(sessions["Peak power (kW)"], errors="coerce")
    start_soc = sessions["Battery level at start (%)"].map(normalize_soc)
    end_soc = sessions["Battery level at end (%)"].map(normalize_soc)

    df_db = pd.DataFrame(
        {
            "charger_id": sessions["charger_id"].astype("Int64"),
            "veh_id": sessions["veh_id"].astype("Int64"),
            "connect_time": connect_utc,
            "disconnect_time": disconnect_utc,
            "tot_ref_dura": duration_min,
            "tot_energy": energy_kwh,
            "max_power": peak_power_kw,
            "start_soc": start_soc,
            "end_soc": end_soc,
        }
    )

    df_db["avg_power"] = (
        (df_db["tot_energy"] * 60.0 / df_db["tot_ref_dura"])
        .where((df_db["tot_ref_dura"] > 0) & df_db["tot_energy"].notna())
        .round(2)
    )

    valid = (
        df_db["connect_time"].notna()
        & df_db["disconnect_time"].notna()
        & (df_db["disconnect_time"] > df_db["connect_time"])
        & (df_db["tot_ref_dura"].fillna(0) > 0)
        & (df_db["tot_energy"].fillna(0) > 0)
    )

    dropped_invalid = (~valid).sum()
    if dropped_invalid:
        print(f"[INFO] Dropping {dropped_invalid} rows with invalid time/duration/energy")

    df_db = df_db[valid].copy()

    cols = [
        "charger_id",
        "veh_id",
        "connect_time",
        "disconnect_time",
        "avg_power",
        "max_power",
        "tot_energy",
        "start_soc",
        "end_soc",
        "tot_ref_dura",
    ]

    df_db = df_db.where(pd.notna(df_db), None)
    rows = [tuple(x) for x in df_db[cols].to_numpy()]

    if not rows:
        print("[INFO] No valid charging rows to upsert.")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            extras.execute_values(cur, INSERT_SQL, rows, page_size=1000)
        conn.commit()

    print(f"[OK] Upserted {len(rows)} rows into refuel_inf")


if __name__ == "__main__":
    main()
