import pandas as pd


FLEET_NAME = "SQ Trucking"

VIN_TO_FLEET_VEHICLE_ID = {
    "4T9BACAA8RB208684": "530043",
    "4T9BACAAXRB208685": "530160",
    "4T9BACAA0RB208761": "532213",
    "4T9BACAA2RB208762": "532234",
    "2G5ZJ3TZ8S9102582": "Chevrolet600",
}

NEW_VEHICLES = [
    {
        "fleet_vehicle_id": "Chevrolet600",
        "make": "Chevrolet",
        "model": "BrightDrop Zevo 600",
        "year": 2025,
    }
]


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = (
        out.columns.astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )
    return out


def normalize_soc(value):
    if pd.isna(value):
        return None
    try:
        value = float(value)
    except Exception:
        return None
    return round(value / 100.0, 4) if value > 1 else round(value, 4)


def nullable_float(value):
    return float(value) if pd.notna(value) else None


def nullable_int(value):
    return int(value) if pd.notna(value) else None


def ensure_sq_vehicles(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM public.fleet WHERE fleet_name = %s", (FLEET_NAME,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Fleet not found: {FLEET_NAME}")
        fleet_id = row[0]

        for vehicle in NEW_VEHICLES:
            cur.execute(
                """
                INSERT INTO public.vehicle (fleet_id, fleet_vehicle_id, make, model, year)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fleet_id, fleet_vehicle_id) DO UPDATE SET
                    make = COALESCE(vehicle.make, EXCLUDED.make),
                    model = COALESCE(vehicle.model, EXCLUDED.model),
                    year = COALESCE(vehicle.year, EXCLUDED.year)
                """,
                (
                    fleet_id,
                    vehicle["fleet_vehicle_id"],
                    vehicle["make"],
                    vehicle["model"],
                    vehicle["year"],
                ),
            )


def load_sq_vehicle_map(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT v.id, v.fleet_vehicle_id
            FROM public.vehicle v
            JOIN public.fleet f ON f.id = v.fleet_id
            WHERE f.fleet_name = %s
            """,
            (FLEET_NAME,),
        )
        return {str(fleet_vehicle_id): vehicle_id for vehicle_id, fleet_vehicle_id in cur.fetchall()}
