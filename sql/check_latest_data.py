import os
import sys

import pandas as pd
from sqlalchemy import bindparam, text

# Ensure project root (one level up from /sql) is on the import path so data_update is found
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from data_update.common_data_update import engine   # SQLAlchemy engine

def _apply_fleet_filter(sql: str, fleets):
    if fleets:
        stmt = text(sql + " WHERE f.fleet_name IN :fleets GROUP BY f.fleet_name ORDER BY f.fleet_name;")
        stmt = stmt.bindparams(bindparam("fleets", expanding=True))
        params = {"fleets": list(fleets)}
    else:
        stmt = text(sql + " GROUP BY f.fleet_name ORDER BY f.fleet_name;")
        params = {}
    return stmt, params


def check_latest_data_tel(table_name, fleets=None):
    base_sql = f"""
        SELECT f.fleet_name, MAX("timestamp")::date as latest_date
        FROM {table_name} AS t
        JOIN vehicle as v
            ON t.veh_id = v.id
        JOIN fleet AS f
            ON v.fleet_id = f.id
    """
    stmt, params = _apply_fleet_filter(base_sql, fleets)
    return pd.read_sql(stmt, engine, params=params)

def check_latest_data_refuel(table_name, fleets=None):
    base_sql = f"""
        SELECT f.fleet_name, MAX(COALESCE(t.connect_time, t.refuel_end, t.refuel_start))::date as latest_date
        FROM {table_name} AS t
        JOIN charger as c
            ON t.charger_id = c.id
        JOIN fleet AS f
            ON c.fleet_id = f.id
    """
    stmt, params = _apply_fleet_filter(base_sql, fleets)
    return pd.read_sql(stmt, engine, params=params)

def check_latest_data(table_name, fleets=None):
    base_sql = f"""
        SELECT f.fleet_name, MAX("date")::date as latest_date
        FROM {table_name} AS t
        JOIN vehicle as v
            ON t.veh_id = v.id
        JOIN fleet AS f
            ON v.fleet_id = f.id
    """
    stmt, params = _apply_fleet_filter(base_sql, fleets)
    return pd.read_sql(stmt, engine, params=params)


def check_latest_data_maintenance(table_name="maintenance", fleets=None):
    base_sql = f"""
        SELECT COALESCE(fv.fleet_name, fc.fleet_name, 'Unspecified') AS fleet_name,
               MAX(t.exit_shop)::date AS latest_date
        FROM {table_name} AS t
        LEFT JOIN vehicle v ON t.veh_id = v.id
        LEFT JOIN fleet fv ON v.fleet_id = fv.id
        LEFT JOIN charger c ON t.charger_id = c.id
        LEFT JOIN fleet fc ON c.fleet_id = fc.id
    """
    if fleets:
        stmt = text(
            base_sql
            + " WHERE COALESCE(fv.fleet_name, fc.fleet_name, 'Unspecified') IN :fleets"
            + " GROUP BY COALESCE(fv.fleet_name, fc.fleet_name, 'Unspecified')"
            + " ORDER BY COALESCE(fv.fleet_name, fc.fleet_name, 'Unspecified');"
        ).bindparams(bindparam("fleets", expanding=True))
        params = {"fleets": list(fleets)}
    else:
        stmt = text(
            base_sql
            + " GROUP BY COALESCE(fv.fleet_name, fc.fleet_name, 'Unspecified')"
            + " ORDER BY COALESCE(fv.fleet_name, fc.fleet_name, 'Unspecified');"
        )
        params = {}
    return pd.read_sql(stmt, engine, params=params)

if __name__ == "__main__":
    print(f'Telematics:\n {check_latest_data_tel("veh_tel")}\n')
    print(f'Refueling Info:\n {check_latest_data_refuel("refuel_inf")}\n') 
    print(f'Vehicle daily usage:\n {check_latest_data("veh_daily")}\n')
    print(f'Maintenance:\n {check_latest_data_maintenance("maintenance")}\n')

