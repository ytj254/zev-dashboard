import psycopg2
import pandas as pd

def get_fleet_data():
    conn = psycopg2.connect(
        dbname="zev_performance",
        user="postgres",
        password="25472",
        host="localhost",
        port=5432
    )
    query = """
        SELECT id, fleet, latitude, longitude, zev_tot, vendor_name, depot_adr
        FROM fleet
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df
