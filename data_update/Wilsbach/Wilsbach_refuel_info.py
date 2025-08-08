import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load .env file (adjust path as needed)
load_dotenv(dotenv_path=r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\zev-dashboard\aws\.env")
db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)

# Load Excel data
excel_path = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\Wilsbach Distributors/EV Data Collection - Charging Event Data.xlsx"
df = pd.read_excel(excel_path)

# Extract prefix before ":" and combine with port
df["charger_id"] = df["Charger ID"].astype(str).str.split(":").str[0] + "-" + df["Port"].astype(str)

# Format and rename columns
df["veh_id"] = df["Vehicle ID"].astype(str)

df = df.rename(columns={
    "Connect Time": "connect_time",
    "Disconnect Time": "disconnect_time",
    "Charge Start Time": "refuel_start",
    "Charge End Time": "refuel_end",
    "Average Power": "avg_power",
    "Peak Power": "max_power",
    "Energy Dispensed (kWh) [Meter]": "tot_energy",
    "Vehicle SoC at start of Charging": "start_soc",
    "Vehicle SoC at end of Charging": "end_soc"
})

# Normalize SoC columns to [0, 1] float values
for col in ["start_soc", "end_soc"]:
    df[col] = df[col].astype(str).str.replace("%", "", regex=False).str.strip()
    df[col] = pd.to_numeric(df[col], errors="coerce") / 100  # Convert to fraction

# Select and reorder relevant columns
df = df[[
    "charger_id", "veh_id", "connect_time", "disconnect_time",
    "refuel_start", "refuel_end", "avg_power", "max_power",
    "tot_energy", "start_soc", "end_soc"
]]

# Insert into refuel_inf (id is auto-incremented)
df.to_sql("refuel_inf", engine, if_exists="append", index=False)

print("Wilsbach charging event data successfully inserted into refuel_inf table.")
