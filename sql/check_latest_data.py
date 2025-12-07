import os
import sys

import pandas as pd
from sqlalchemy.sql import text

# Ensure project root (one level up from /sql) is on the import path so data_update is found
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data_update.common_data_update import engine   # SQLAlchemy engine

def check_latest_data_tel(table_name, fleets=None):
    sql = f"""
        SELECT f.fleet_name, MAX("timestamp")::date as latest_date
        FROM {table_name} AS t
        JOIN vehicle as v
            ON t.veh_id = v.id
        JOIN fleet AS f
            ON v.fleet_id = f.id
        { "WHERE f.fleet_name IN :fleets" if fleets else "" }
        GROUP BY f.fleet_name
        ORDER BY latest_date desc;
    """

    params = {"fleets": tuple(fleets)} if fleets else {}
    return pd.read_sql(text(sql), engine, params=params)

def check_latest_data_refuel(table_name, fleets=None):
    sql = f"""
        SELECT f.fleet_name, MAX("connect_time")::date as latest_date
        FROM {table_name} AS t
        JOIN vehicle as v
            ON t.veh_id = v.id
        JOIN fleet AS f
            ON v.fleet_id = f.id
        { "WHERE f.fleet_name IN :fleets" if fleets else "" }
        GROUP BY f.fleet_name
        ORDER BY latest_date desc;
    """

    params = {"fleets": tuple(fleets)} if fleets else {}
    return pd.read_sql(text(sql), engine, params=params)

def check_latest_data(table_name, fleets=None):
    sql = f"""
        SELECT f.fleet_name, MAX("date")::date as latest_date
        FROM {table_name} AS t
        JOIN vehicle as v
            ON t.veh_id = v.id
        JOIN fleet AS f
            ON v.fleet_id = f.id
        { "WHERE f.fleet_name IN :fleets" if fleets else "" }
        GROUP BY f.fleet_name
        ORDER BY latest_date desc;
    """

    params = {"fleets": tuple(fleets)} if fleets else {}
    return pd.read_sql(text(sql), engine, params=params)

if __name__ == "__main__":
    print(f'Telematics:\n {check_latest_data_tel("veh_tel")}\n')
    print(f'Refueling Info:\n {check_latest_data_refuel("refuel_inf")}\n') 
    print(f'Vehicle daily usage:\n {check_latest_data("veh_daily")}\n')
    print(f'Maintenance:\n {check_latest_data("maintenance")}\n')

