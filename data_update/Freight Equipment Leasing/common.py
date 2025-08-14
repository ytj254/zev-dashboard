import os, hashlib, shutil, re
from datetime import datetime
from pathlib import Path
import json

import pandas as pd
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras as _extras

# ---- CONFIG ----
FREIGHT_VEH_IDS = {"DSE175","DSE176","DSE177","SSE26116","SE28500","SE28501"}
FLEET_NAME = "Freight Equipment Leasing"
ROOT_DIR = r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Freight Equipment Leasing\aws_download"
ARCHIVE_SUB = "_archive"
LOG_FILE = Path(__file__).parent / "_ingestion_log.json"

# Load .env from your absolute path
load_dotenv(dotenv_path=r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\zev-dashboard\aws\.env")
DATABASE_URL = os.getenv("DATABASE_URL")

DATEFOLDER_RE = re.compile(r"^\d{8}$")  # YYYYMMDD

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def md5_file(p: Path) -> str:
    h = hashlib.md5()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _load_local_log():
    if LOG_FILE.exists():
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_local_log(data):
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def already_ingested(conn, file_path: Path, file_hash: str) -> bool:
    """
    Check if a file with this hash has already been ingested.
    """
    log_data = _load_local_log()
    return log_data.get(str(file_path)) == file_hash

def record_ingestion(conn, file_path: Path, file_hash: str, rows_loaded: int):
    """
    Record the file's ingestion into the local log.
    """
    log_data = _load_local_log()
    log_data[str(file_path)] = file_hash
    _save_local_log(log_data)

def get_fleet_id_and_vehicle_maps(conn):
    """
    Returns:
      fleet_id: int
      str2int: {fleet_vehicle_id(str) -> vehicle.id(int)} ONLY for the six whitelisted IDs.
    """
    with conn.cursor(cursor_factory=_extras.RealDictCursor) as cur:
        cur.execute("SELECT id FROM public.fleet WHERE fleet_name=%s", (FLEET_NAME,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Fleet not found: {FLEET_NAME}")
        fleet_id = row["id"]

        cur.execute("""
            SELECT id, fleet_vehicle_id
            FROM public.vehicle
            WHERE fleet_id=%s
        """, (fleet_id,))
        str2int = {}
        for r in cur.fetchall():
            fid = (r["fleet_vehicle_id"] or "").strip()
            if fid in FREIGHT_VEH_IDS:
                str2int[fid] = r["id"]
        missing = FREIGHT_VEH_IDS - set(str2int.keys())
        if missing:
            raise RuntimeError(f"Missing vehicles in DB for fleet '{FLEET_NAME}': {sorted(missing)}")
        return fleet_id, str2int

def get_charger_map(conn, fleet_id: int):
    """
    Returns {charger_name(str) -> charger.id(int)} for the given fleet_id.
    """
    with conn.cursor(cursor_factory=_extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, charger
            FROM public.charger
            WHERE fleet_id=%s
        """, (fleet_id,))
        return { (r["charger"] or "").strip(): r["id"] for r in cur.fetchall() }

def ensure_archive(folder: Path) -> Path:
    arc = folder / ARCHIVE_SUB
    arc.mkdir(exist_ok=True)
    return arc

def move_to_archive(src: Path, archive_root: Path):
    dst = archive_root / src.name
    # Avoid overwriting: add timestamp suffix if needed
    if dst.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = archive_root / f"{src.stem}__{stamp}{src.suffix}"
    shutil.move(str(src), str(dst))

def list_date_subfolders(root: Path):
    for p in root.iterdir():
        if p.is_dir() and DATEFOLDER_RE.match(p.name):
            yield p

def is_monthly_folder(folder: Path) -> bool:
    return (folder / "AO_Daily_Summary.xlsx").exists()

def is_weekly_folder(folder: Path) -> bool:
    return any(p.suffix.lower()==".csv" for p in folder.iterdir() if p.is_file())

def normalize_soc(x):
    if pd.isna(x):
        return None
    try:
        v = float(x)
    except Exception:
        return None
    # If looks like 0-100, convert to 0-1
    if v > 1.000001:
        v = v / 100.0
    # Clamp
    if v < 0: v = 0.0
    if v > 1: v = 1.0
    return round(v, 4)

def minutes_to_hours(x):
    if pd.isna(x):
        return None
    try:
        return round(float(x)/60.0, 2)
    except Exception:
        return None

def round_int(x):
    if pd.isna(x):
        return None
    try:
        return int(round(float(x)))
    except Exception:
        return None
