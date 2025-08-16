
import pandas as pd
import re
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data_update.common_data_update import engine


# Replace this with the actual Excel file path
excel_path = "D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\Incoming fleet data\SQ Trucking\SQ Trucking EVs Data for February 2025.xls"

# Load data
df = pd.read_excel(excel_path, engine="xlrd")

# Extract veh_id from Nickname (retain only numeric part)
df["veh_id"] = df["Nickname"].apply(lambda x: "".join(re.findall(r'\d+', str(x))))

# Rename and filter relevant columns
veh_daily_df = df[[
    "veh_id",
    "Date",
    "Distance Driven",
    "Time In Service",
    "SOC Used",
    "Energy Used"
]].rename(columns={
    "Date": "date",
    "Distance Driven": "tot_dist",
    "Time In Service": "tot_dura",
    "SOC Used": "tot_soc_used",
    "Energy Used": "tot_energy"
})

# Add required placeholder columns
veh_daily_df["trip_num"] = None
veh_daily_df["init_odo"] = None
veh_daily_df["final_odo"] = None
veh_daily_df["idle_time"] = None
veh_daily_df["init_soc"] = None
veh_daily_df["final_soc"] = None
veh_daily_df["peak_payload"] = None

# Reorder columns to match database schema
veh_daily_df = veh_daily_df[[
    "veh_id", "date", "trip_num", "init_odo", "final_odo", "tot_dist",
    "tot_dura", "idle_time", "init_soc", "final_soc", "tot_soc_used",
    "tot_energy", "peak_payload"
]]

# Convert units like "106 kWh" to numeric
veh_daily_df["tot_energy"] = veh_daily_df["tot_energy"].str.replace(" kWh", "", regex=False).astype(float)
veh_daily_df["tot_dura"] = veh_daily_df["tot_dura"].str.replace(" h", "", regex=False).astype(float)
veh_daily_df["tot_dist"] = veh_daily_df["tot_dist"].astype(float)
veh_daily_df["tot_soc_used"] = veh_daily_df["tot_soc_used"].astype(float)

# Upload to database
veh_daily_df.to_sql("veh_daily", engine, if_exists="append", index=False)

print("SQ daily data successfully inserted into veh_daily table.")