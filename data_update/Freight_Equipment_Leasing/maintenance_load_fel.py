from pathlib import Path
import re
import pandas as pd
import psycopg2.extras as extras
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from data_update.common_data_update import get_conn
from data_update.utils import to_boolean
from common import FLEET_NAME, get_fleet_id_and_vehicle_maps, get_charger_map


# ==================== Config ====================
FOLDER_PATH = Path(
    r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Freight Equipment Leasing\maintenance data"
)
VEH_FILE = FOLDER_PATH / "data collect HBG Maintenance Events.xlsx"
CHARGER_FILE = FOLDER_PATH / "data collect HBG CHARGER Maintenance Events.xlsx"

MAINT_OB_VEHICLE = 1
MAINT_OB_CHARGER = 2
STATION_LEVEL_NOTE = "[Station-level C03]"

COL_ASSET = "Vehicle ID (unique identifier)"
COL_CATEG = "Maintenance category (identify all that apply) -  routine preventive maintenance, diagnostic, repair \n"
COL_PROBLEM = "If diagnostic or repair work, description of the condition or problem"
COL_WORK = "Description of the work performed"
COL_LOC = "Maintenance work performed in-house or outsourced?"
COL_ENTER = "The timestamp when vehicle entered the shop (yyyy-mm-dd hh24:mm)"
COL_EXIT = "The timestamp when vehicle exited the shop (yyyy-mm-dd hh24:mm)"
COL_ENTER_ODO = "Odometer reading upon entering shop (miles)"
COL_EXIT_ODO = "Odometer reading upon exiting shop (miles)"
COL_PARTS = "Parts cost ($)"
COL_LABOR = "Labor cost ($)"
COL_ADD = "Additional costs, if any ($) (please describe)"
COL_WARRANTY = "Warranty covered (yes or no)."

INSTRUCTION_SHEETS = {"MAINTENANCE", "CHARGER MAINTENANCE"}

INSERT_COLS = [
    "date",
    "maint_ob",
    "maint_categ",
    "maint_loc",
    "enter_shop",
    "exit_shop",
    "enter_odo",
    "exit_odo",
    "parts_cost",
    "labor_cost",
    "add_cost",
    "warranty",
    "problem",
    "work_perf",
    "charger_id",
    "veh_id",
    "add_cost_desc",
]


def _parse_money(value):
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    m = re.search(r"-?\d[\d,]*(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except Exception:
        return None


def _parse_additional_cost(value):
    if pd.isna(value):
        return None, None
    s = str(value).strip()
    if not s:
        return None, None

    numeric = _parse_money(s)
    if numeric is None:
        return None, s

    cleaned = re.sub(r"-?\d[\d,]*(?:\.\d+)?", "", s, count=1)
    cleaned = cleaned.replace("$", "").strip()
    cleaned = cleaned.strip("() ")
    desc = cleaned if cleaned else None
    return numeric, desc


def _split_category_and_problem(categ_value, problem_value):
    c = "" if pd.isna(categ_value) else str(categ_value)
    p = "" if pd.isna(problem_value) else str(problem_value)

    if ":" in c:
        left, right = c.split(":", 1)
        maint_categ = left.strip() or None
        prefix = right.strip()
        if prefix and p:
            problem = f"{prefix}: {p}"
        elif prefix:
            problem = prefix
        else:
            problem = p.strip() or None
    else:
        maint_categ = c.strip() or None
        problem = p.strip() or None

    return maint_categ, problem


def _normalize_sheet(df):
    d = df.copy()
    d.columns = d.columns.astype(str).str.strip()
    d.columns = d.columns.str.replace("\n", " ", regex=False)
    d.columns = d.columns.str.replace(r"\s+", " ", regex=True).str.strip()
    d = d.dropna(how="all")
    return d


def _resolve_columns(df: pd.DataFrame) -> dict[str, str]:
    # Robust matching for vendor files where whitespace/newlines can vary.
    col_map = {}
    cols = list(df.columns)

    def find_one(*patterns: str) -> str | None:
        for c in cols:
            lc = c.lower()
            if all(p in lc for p in patterns):
                return c
        return None

    col_map["asset"] = find_one("vehicle id", "unique")
    col_map["categ"] = find_one("maintenance category")
    col_map["problem"] = find_one("description of the condition or problem")
    col_map["work"] = find_one("description of the work performed")
    col_map["loc"] = find_one("in-house or outsourced")
    col_map["enter"] = find_one("entered the shop")
    col_map["exit"] = find_one("exited the shop")
    col_map["enter_odo"] = find_one("odometer reading upon entering")
    col_map["exit_odo"] = find_one("odometer reading upon exiting")
    col_map["parts"] = find_one("parts cost")
    col_map["labor"] = find_one("labor cost")
    col_map["add"] = find_one("additional costs")
    col_map["warranty"] = find_one("warranty covered")
    return col_map


def _load_workbook(path: Path, asset_type: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Maintenance file not found: {path}")

    all_rows = []
    with pd.ExcelFile(path, engine="openpyxl") as xl:
        for sheet in xl.sheet_names:
            if sheet.strip().upper() in INSTRUCTION_SHEETS:
                continue

            raw = _normalize_sheet(xl.parse(sheet_name=sheet))
            if raw.empty:
                continue

            resolved = _resolve_columns(raw)
            missing = [k for k, v in resolved.items() if v is None]
            if missing:
                raise RuntimeError(f"Missing columns in {path.name} [{sheet}]: {sorted(missing)}")

            out = pd.DataFrame()
            out["asset_code"] = raw[resolved["asset"]].astype(str).str.strip()
            out["enter_shop"] = pd.to_datetime(raw[resolved["enter"]], errors="coerce")
            out["exit_shop"] = pd.to_datetime(raw[resolved["exit"]], errors="coerce")
            out["enter_odo"] = pd.to_numeric(raw[resolved["enter_odo"]], errors="coerce").round().astype("Int64")
            out["exit_odo"] = pd.to_numeric(raw[resolved["exit_odo"]], errors="coerce").round().astype("Int64")
            out["parts_cost"] = raw[resolved["parts"]].apply(_parse_money)
            out["labor_cost"] = raw[resolved["labor"]].apply(_parse_money)

            add_vals = raw[resolved["add"]].apply(_parse_additional_cost)
            out["add_cost"] = add_vals.map(lambda x: x[0])
            out["add_cost_desc"] = add_vals.map(lambda x: x[1])

            out["warranty"] = to_boolean(raw[resolved["warranty"]])
            out["maint_loc"] = raw[resolved["loc"]].astype(str).str.strip()
            out["work_perf"] = raw[resolved["work"]].astype(str).str.strip().replace({"nan": None, "": None})

            parsed = raw.apply(
                lambda r: _split_category_and_problem(r[resolved["categ"]], r[resolved["problem"]]),
                axis=1,
            )
            out["maint_categ"] = parsed.map(lambda x: x[0])
            out["problem"] = parsed.map(lambda x: x[1])

            out["date"] = out["enter_shop"].dt.date
            out["asset_type"] = asset_type
            out["source_sheet"] = sheet

            # User requirement: keep station-level C03 rows with NULL charger_id and explicit note.
            if asset_type == "charger":
                mask_c03 = out["asset_code"].eq("C03")
                if mask_c03.any():
                    out.loc[mask_c03, "problem"] = out.loc[mask_c03, "problem"].fillna("").apply(
                        lambda s: (f"{s} {STATION_LEVEL_NOTE}".strip() if STATION_LEVEL_NOTE not in s else s)
                    )

            all_rows.append(out)

    if not all_rows:
        return pd.DataFrame(columns=["asset_code", "date", "maint_ob", "veh_id", "charger_id"] + INSERT_COLS)

    return pd.concat(all_rows, ignore_index=True)


def _to_compare_frame(df: pd.DataFrame) -> pd.DataFrame:
    c = df.copy()
    for col in INSERT_COLS:
        if col not in c.columns:
            c[col] = None

        s = c[col]
        if pd.api.types.is_datetime64_any_dtype(s):
            c[col] = s.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("__NULL__")
        else:
            c[col] = s.where(pd.notna(s), "__NULL__").astype(str).str.strip()

    return c[INSERT_COLS]


def _filter_new_rows(conn, incoming: pd.DataFrame) -> pd.DataFrame:
    if incoming.empty:
        return incoming

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                date, maint_ob, maint_categ, maint_loc, enter_shop, exit_shop,
                enter_odo, exit_odo, parts_cost, labor_cost, add_cost, warranty,
                problem, work_perf, charger_id, veh_id, add_cost_desc
            FROM public.maintenance
            """
        )
        existing_rows = cur.fetchall()

    existing = pd.DataFrame(existing_rows, columns=INSERT_COLS)
    incoming_cmp = _to_compare_frame(incoming)
    existing_cmp = _to_compare_frame(existing)

    # De-dup inside incoming batch first.
    incoming_cmp["_row_idx"] = range(len(incoming_cmp))
    incoming_cmp = incoming_cmp.drop_duplicates(subset=INSERT_COLS, keep="first")

    merged = incoming_cmp.merge(
        existing_cmp.drop_duplicates(subset=INSERT_COLS),
        on=INSERT_COLS,
        how="left",
        indicator=True,
    )

    keep_idx = merged.loc[merged["_merge"] == "left_only", "_row_idx"].tolist()
    return incoming.iloc[keep_idx].copy()


def main():
    veh_df = _load_workbook(VEH_FILE, asset_type="vehicle")
    chg_df = _load_workbook(CHARGER_FILE, asset_type="charger")

    with get_conn() as conn:
        fleet_id, veh_map = get_fleet_id_and_vehicle_maps(conn)
        charger_map = get_charger_map(conn, fleet_id)

        veh_df["veh_id"] = veh_df["asset_code"].map(veh_map).astype("Int64")
        veh_df["charger_id"] = pd.Series([None] * len(veh_df), dtype="object")
        veh_df["maint_ob"] = MAINT_OB_VEHICLE

        chg_df["charger_id"] = chg_df["asset_code"].map(charger_map).astype("Int64")
        chg_df["veh_id"] = pd.Series([None] * len(chg_df), dtype="object")
        # User requirement: charger maintenance gets charger maint_ob bucket.
        chg_df["maint_ob"] = MAINT_OB_CHARGER

        # Keep C03 as station-level with NULL charger_id; drop other unknown charger codes.
        non_c03_unknown = chg_df["charger_id"].isna() & (~chg_df["asset_code"].eq("C03"))
        dropped_unknown_charger = int(non_c03_unknown.sum())
        if dropped_unknown_charger:
            chg_df = chg_df.loc[~non_c03_unknown].copy()

        dropped_unmapped_vehicle = int(veh_df["veh_id"].isna().sum())
        if dropped_unmapped_vehicle:
            veh_df = veh_df.loc[veh_df["veh_id"].notna()].copy()

        frames = [f for f in (veh_df, chg_df) if not f.empty]
        if not frames:
            print("[INFO] No parsed maintenance rows after mapping.")
            return
        combined = pd.concat(frames, ignore_index=True)

        cols_needed = INSERT_COLS.copy()
        combined = combined[cols_needed].copy()
        combined = combined.astype(object).where(pd.notna(combined), None)

        to_insert = _filter_new_rows(conn, combined)
        rows = [tuple(x) for x in to_insert[INSERT_COLS].to_numpy()]

        inserted = 0
        if rows:
            sql = """
            INSERT INTO public.maintenance (
                date, maint_ob, maint_categ, maint_loc, enter_shop, exit_shop,
                enter_odo, exit_odo, parts_cost, labor_cost, add_cost, warranty,
                problem, work_perf, charger_id, veh_id, add_cost_desc
            )
            VALUES %s
            ON CONFLICT DO NOTHING
            RETURNING id
            """
            with conn.cursor() as cur:
                ret = extras.execute_values(cur, sql, rows, page_size=1000, fetch=True)
                inserted = len(ret) if ret is not None else 0
            conn.commit()

    print("=== FEL Maintenance Upload Summary ===")
    print(f"Vehicle rows read:                {len(veh_df)}")
    print(f"Charger rows read:                {len(chg_df)}")
    print(f"Dropped unmapped vehicle rows:    {dropped_unmapped_vehicle}")
    print(f"Dropped unknown charger rows:     {dropped_unknown_charger}")
    print(f"Rows after in-batch/existing dedup: {len(to_insert)}")
    print(f"Inserted new rows:                {inserted}")


if __name__ == "__main__":
    main()
