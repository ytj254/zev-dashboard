# ZEV Fleet Dashboard

This project is a web-based dashboard for exploring fleet data related to zero-emission vehicles (ZEVs) in Pennsylvania. It visualizes fleet locations on a map and shows detailed information about each fleet’s vehicles and chargers. The goal is to help users, such as DEP staff or researchers, understand the makeup and distribution of ZEV fleets across the state.

---

## How the App Is Structured

- `app.py`: Entry point that launches the Dash multi-page app
- `pages/...`: Contains pages for each dashboard view:
  - `overview.py`: Landing page
  - `fleet_info.py`: Fleet map and details
  - `charging.py`: Charger usage and types
  - `telematics.py`: Telematics and trip summaries
  - `veh_daily_usage.py`: Daily usage per vehicle
  - `analysis.py`: Summary analysis and visualizations
- `db.py`: Functions to load data from PostgreSQL (fleets, vehicles, chargers, etc.)
- `utils.py`: Stores mappings for vehicle and charger types and helper functions
- `assets/...`: Contains assets:
  - `background.png`: Background picture for the overview page
  - `pa_boundary.geojson`: GeoJSON file used to draw the Pennsylvania state boundary on the map

> The project uses `dash`, `dash-bootstrap-components`, `dash-leaflet`, `pandas`, and `sqlalchemy`.

---

## How to Run

1. Set up a PostgreSQL database with ZEV fleet operation data:

2. Update the database connection info in `db.py`

3. Install dependencies:
   ```bash
   pip install -r requirements.txt