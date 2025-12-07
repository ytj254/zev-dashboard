import os
import sys

import pandas as pd
from sqlalchemy.sql import text

# Ensure project root (one level up from /sql) is on the import path so data_update is found
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data_update.common_data_update import engine   # SQLAlchemy engine

def count_by_fleet(table_name, fleets=None):
    sql = f"""
        SELECT f.fleet_name, COUNT(*) AS n_points
        FROM {table_name} AS t
        JOIN vehicle as v
            ON t.veh_id = v.id
        JOIN fleet AS f
            ON v.fleet_id = f.id
        { "WHERE f.fleet_name IN :fleets" if fleets else "" }
        GROUP BY f.fleet_name
        ORDER BY f.fleet_name;
    """

    params = {"fleets": tuple(fleets)} if fleets else {}
    return pd.read_sql(text(sql), engine, params=params)

if __name__ == "__main__":
    print(f'Telematics:\n {count_by_fleet("veh_tel")}\n')
    print(f'Refueling Info:\n {count_by_fleet("refuel_inf")}\n') 
    print(f'Vehicle daily usage:\n {count_by_fleet("veh_daily")}\n')
    print(f'Maintenance:\n {count_by_fleet("maintenance")}\n')