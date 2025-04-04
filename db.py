from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("postgresql://postgres:25472@localhost:5432/zev_performance")

def get_fleet_data():
    query = """
        SELECT id, fleet_name, fleet_size, zev_tot, zev_grant, charger_grant, depot_adr, vendor_name, latitude, longitude
        FROM fleet
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    return pd.read_sql(query, engine)
