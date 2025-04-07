from sqlalchemy import create_engine
import pandas as pd
import os

db_url = os.getenv("DATABASE_URL", "postgresql://postgres:25472@localhost:5432/zev_performance")
engine = create_engine(db_url)

def get_fleet_data():
    query = """
        SELECT id, fleet_name, fleet_size, zev_tot, zev_grant, charger_grant, depot_adr, vendor_name, latitude, longitude
        FROM fleet
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    return pd.read_sql(query, engine)

def get_veh_data():
    query = """
        SELECT id, fleet_id, fleet_vehicle_id, make, model, year, class, curb_wt, gross_wt, rated_cap, nominal_range, nominal_eff, battery_chem, peak_power, peak_torque, towing_cap, vocation
        FROM vehicle
    """
    return pd.read_sql(query, engine)

def get_charger_data():
    query = """
        SELECT id, fleet_id, charger, charger_type, connector_type, max_power_output, dedicated_use
        FROM charger
    """
    return pd.read_sql(query, engine)


if __name__ == "__main__":
    df = get_charger_data()
    print(df)