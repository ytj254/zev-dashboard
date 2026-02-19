from pathlib import Path
import pandas as pd
import psycopg2.extras as _extras
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from data_update.common_data_update import get_conn
from common import FREIGHT_VEH_IDS, get_fleet_id_and_vehicle_maps


EXCEL_FILE = Path(
    r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Freight Equipment Leasing\Daily payload\data collect HBG Daily Summary.xlsx"
)

COL_VEH = "(1) Vehicle ID (unique vehicle identifier)"
COL_DATE = "(2) Date (yyyy-mm-dd)"
COL_PAYLOAD = "(11) Peak payload of the day (lbs)"


def parse_payload_sheet(sheet_df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    df = sheet_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    required = {COL_DATE, COL_PAYLOAD}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing columns in sheet {sheet_name}: {sorted(missing)}")

    veh_series = (
        df[COL_VEH].astype(str).str.strip()
        if COL_VEH in df.columns
        else pd.Series([sheet_name] * len(df), index=df.index)
    )

    out = pd.DataFrame(
        {
            "fleet_vehicle_id": veh_series,
            "date": pd.to_datetime(df[COL_DATE], errors="coerce").dt.date,
            "peak_payload": pd.to_numeric(df[COL_PAYLOAD], errors="coerce"),
        }
    )

    out = out.dropna(subset=["date", "peak_payload"]).copy()
    out = out[out["peak_payload"] > 0].copy()
    out["peak_payload"] = out["peak_payload"].round().astype("Int64")

    # Business rule: for duplicate vehicle-date entries in a file, keep max payload.
    out = (
        out.groupby(["fleet_vehicle_id", "date"], as_index=False)["peak_payload"]
        .max()
    )
    return out


def upsert_payload(conn, rows: list[tuple[int, object, int]]) -> None:
    with conn.cursor() as cur:
        _extras.execute_values(
            cur,
            """
            INSERT INTO public.veh_daily (veh_id, date, peak_payload)
            VALUES %s
            ON CONFLICT (veh_id, date) DO UPDATE SET
                peak_payload = EXCLUDED.peak_payload
            WHERE veh_daily.peak_payload IS DISTINCT FROM EXCLUDED.peak_payload
            """,
            rows,
            page_size=500,
        )


def main() -> None:
    if not EXCEL_FILE.exists():
        raise FileNotFoundError(f"Payload file not found: {EXCEL_FILE}")

    parsed_frames = []
    with pd.ExcelFile(EXCEL_FILE, engine="openpyxl") as xl:
        for sheet in xl.sheet_names:
            if sheet not in FREIGHT_VEH_IDS:
                continue
            df_sheet = xl.parse(sheet_name=sheet)
            parsed = parse_payload_sheet(df_sheet, sheet)
            parsed_frames.append(parsed)

    if not parsed_frames:
        print("[INFO] No vehicle payload sheets found.")
        return

    payload_df = pd.concat(parsed_frames, ignore_index=True)

    with get_conn() as conn:
        _, veh_map = get_fleet_id_and_vehicle_maps(conn)

        before_map = len(payload_df)
        payload_df = payload_df[payload_df["fleet_vehicle_id"].isin(veh_map.keys())].copy()
        print(f"[INFO] Dropped {before_map - len(payload_df)} rows with unmapped vehicle IDs")

        payload_df["veh_id"] = payload_df["fleet_vehicle_id"].map(veh_map).astype("Int64")
        payload_df = (
            payload_df.groupby(["veh_id", "date"], as_index=False)["peak_payload"]
            .max()
        )

        rows = [
            (int(r.veh_id), r.date, int(r.peak_payload))
            for r in payload_df.itertuples(index=False)
        ]

        if not rows:
            print("[INFO] No valid payload rows to upsert.")
            return

        upsert_payload(conn, rows)
        conn.commit()

    print(
        f"[OK] Upserted payload for {len(rows)} vehicle-date rows "
        f"from {payload_df['date'].min()} to {payload_df['date'].max()}"
    )


if __name__ == "__main__":
    main()
